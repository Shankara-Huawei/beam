/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.redis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>RedisIO.Read provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example
 * illustrates how to configure a source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnectionConfiguration(
 *      RedisConnectionConfiguration.create().withHost("localhost").withPort(6379))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RedisIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIO.class);

  /**
   * Read data from a Redis server.
   */
  public static Read read() {
    return new AutoValue_RedisIO_Read.Builder().setKeyPattern("*").build();
  }

  /**
   * Like {@link #read()} but executes multiple instances of the Redis query substituting each
   * element of a {@link PCollection} as key pattern.
   */
  public static ReadAll readAll() {
    return new AutoValue_RedisIO_ReadAll.Builder().build();
  }

  private RedisIO() {
  }

  /**
   * Implementation of {@link #read()}.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, String>>> {

    @Nullable abstract RedisConnectionConfiguration connectionConfiguration();
    @Nullable abstract String keyPattern();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);
      @Nullable abstract Builder setKeyPattern(String keyPattern);
      abstract Read build();
    }

    /**
     * Define the connectionConfiguration to the Redis server.
     *
     * @param connection The {@link RedisConnectionConfiguration}.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "RedisIO.read().withConnectionConfiguration"
          + "(connectionConfiguration) called with null connectionConfiguration");
      return builder().setConnectionConfiguration(connection).build();
    }

    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "RedisIO.read().withKeyPattern(keyPattern)"
          + "called with null keyPattern");
      return builder().setKeyPattern(keyPattern).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(connectionConfiguration() != null,
          "RedisIO.read() requires a connectionConfiguration to be set "
              + "withConnection(connectionConfiguration)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      if (connectionConfiguration() != null) {
        connectionConfiguration().populateDisplayData(builder);
      }
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
      return input
          .apply(Create.of(keyPattern()))
          .apply(RedisIO.readAll().withConnectionConfiguration(connectionConfiguration()));
    }

  }

  /**
   * Implementation of {@link #readAll()}.
   */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    @Nullable abstract RedisConnectionConfiguration connectionConfiguration();

    abstract ReadAll.Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract ReadAll.Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);
      abstract ReadAll build();
    }

    public ReadAll withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "RedisIO.read().withConnectionConfiguration"
          + "(connectionConfiguration) called with null connectionConfiguration");
      return builder().setConnectionConfiguration(connection).build();
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
      return input
          .apply(ParDo.of(
              new ReadFn(connectionConfiguration())
          ))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
      // reparallelized ?
    }

  }

  /**
   * A {@link DoFn} requesting Redis server to get key/value pairs.
   */
  private static class ReadFn extends DoFn<String, KV<String, String>> {

    private final RedisConnectionConfiguration connectionConfiguration;

    private transient Jedis jedis;

    public ReadFn(RedisConnectionConfiguration connectionConfiguration) {
      this.connectionConfiguration = connectionConfiguration;
    }

    @Setup
    public void setup() {
      jedis = connectionConfiguration.connect();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      ScanParams scanParams = new ScanParams();
      scanParams.match(processContext.element());

      String cursor = ScanParams.SCAN_POINTER_START;
      boolean finished = false;
      while (!finished) {
        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
        List<String> keys = scanResult.getResult();

        Pipeline pipeline = jedis.pipelined();
        if (keys != null) {
          for (String key : keys) {
            pipeline.get(key);
          }
          List<Object> values = pipeline.syncAndReturnAll();
          for (int i = 0; i < values.size(); i++) {
            processContext.output(KV.of(keys.get(i), (String) values.get(i)));
          }
        }

        cursor = scanResult.getStringCursor();
        if (cursor.equals("0")) {
          finished = true;
        }
      }
    }

    @Teardown
    public void teardown() {
      jedis.close();
    }

  }

}
