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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

/**
 * Test on the Redis IO.
 */
public class RedisIOTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetRead() throws Exception {
    try (EmbeddedRedis embeddedRedis = new EmbeddedRedis()) {
      ArrayList<KV<String, String>> expected = new ArrayList<>();
      Jedis jedis = new Jedis("localhost", embeddedRedis.getPort());
      for (int i = 0; i < 1000; i++) {
        KV<String, String> kv = KV.of("Key " + i, "Value " + i);
        jedis.set(kv.getKey(), kv.getValue());
        expected.add(kv);
      }
      jedis.quit();

      PCollection<KV<String, String>> readAll = pipeline.apply(
          RedisIO.read()
              .withConnectionConfiguration(RedisConnectionConfiguration.create()
                  .withHost("localhost")
                  .withPort(embeddedRedis.getPort())));

      PAssert.thatSingleton(readAll.apply("Count", Count.<KV<String, String>>globally()))
          .isEqualTo(1000L);
      PAssert.that(readAll).containsInAnyOrder(expected);

      pipeline.run();
    }
  }

  @Test
  public void testGetReadWithKeyPattern() throws Exception {
    try (EmbeddedRedis embeddedRedis = new EmbeddedRedis()) {
      Jedis jedis = new Jedis("localhost", embeddedRedis.getPort());
      for (int i = 0; i < 1000; i++) {
        KV<String, String> kv = KV.of("Key " + i, "Value " + i);
        jedis.set(kv.getKey(), kv.getValue());
      }
      jedis.quit();

      PCollection<KV<String, String>> read = pipeline.apply(
          RedisIO.read()
            .withConnectionConfiguration(RedisConnectionConfiguration.create()
              .withHost("localhost")
              .withPort(embeddedRedis.getPort()))
            .withKeyPattern("Key 0"));

      PAssert.thatSingleton(read.apply("Count", Count.<KV<String, String>>globally()))
          .isEqualTo(1L);

      pipeline.run();
    }
  }

  @Test
  public void testGetReadAll() throws Exception {
    try (EmbeddedRedis embeddedRedis = new EmbeddedRedis()) {
      Jedis jedis = new Jedis("localhost", embeddedRedis.getPort());
      for (int i = 0; i < 1000; i++) {
        KV<String, String> kv = KV.of("Key " + i, "Value " + i);
        jedis.set(kv.getKey(), kv.getValue());
      }
      jedis.quit();

      PCollection<KV<String, String>> readAll =
          pipeline.apply(Create.<String>of("Key 0", "Key 1"))
          .apply(RedisIO.readAll()
              .withConnectionConfiguration(RedisConnectionConfiguration.create()
                .withHost("localhost")
                .withPort(embeddedRedis.getPort())));

      PAssert.thatSingleton(readAll.apply("Count", Count.<KV<String, String>>globally()))
          .isEqualTo(2L);

      pipeline.run();
    }
  }

  @Test
  public void testConnectionInfiniteTimeout() throws Exception {
    try (EmbeddedRedis embeddedRedis = new EmbeddedRedis()) {
      ArrayList<KV<String, String>> expected = new ArrayList<>();
      Jedis jedis = new Jedis("localhost", embeddedRedis.getPort());
      for (int i = 0; i < 1000; i++) {
        KV<String, String> kv = KV.of("Key " + i, "Value " + i);
        jedis.set(kv.getKey(), kv.getValue());
        expected.add(kv);
      }
      jedis.quit();

      PCollection<KV<String, String>> read = pipeline.apply(
          RedisIO.read()
              .withConnectionConfiguration(RedisConnectionConfiguration.create()
                  .withHost("localhost")
                  .withPort(embeddedRedis.getPort())
                  .withTimeout(0)));

      PAssert.thatSingleton(read.apply("Count", Count.<KV<String, String>>globally()))
          .isEqualTo(1000L);
      PAssert.that(read).containsInAnyOrder(expected);

      pipeline.run();
    }
  }

  /**
   * Simple embedded Redis instance wrapper to control Redis server.
   */
  private static class EmbeddedRedis implements AutoCloseable {

    private final int port;
    private final RedisServer redisServer;

    public EmbeddedRedis() throws IOException {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        port = serverSocket.getLocalPort();
      }
      redisServer = new RedisServer(port);
      redisServer.start();
    }

    public int getPort() {
      return this.port;
    }

    @Override
    public void close() {
      redisServer.stop();
    }

  }

}
