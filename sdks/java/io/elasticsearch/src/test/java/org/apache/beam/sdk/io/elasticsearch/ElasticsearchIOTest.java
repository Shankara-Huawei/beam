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
package org.apache.beam.sdk.io.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

/**
 * Test on {@link ElasticsearchIO}.
 */
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String DATA_DIRECTORY = "target/elasticsearch";
  public static final String ES_INDEX = "beam";
  public static final String ES_TYPE = "test";
  private static final String ES_IP = "localhost";
  private static final String ES_HTTP_PORT = "9201";
  private static final String ES_TCP_PORT = "9301";
  private static final long NB_DOCS = 400L;

  private static transient Node node;

  @BeforeClass
  public static void beforeClass() throws IOException {
    FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    LOGGER.info("Starting embedded Elasticsearch instance");
    Settings.Builder settingsBuilder =
        Settings.settingsBuilder()
            .put("cluster.name", "beam")
            .put("http.enabled", "true")
            .put("node.data", "true")
            .put("path.data", DATA_DIRECTORY)
            .put("path.home", DATA_DIRECTORY)
            .put("node.name", "beam")
            .put("network.host", ES_IP)
            .put("port", ES_TCP_PORT)
            .put("http.port", ES_HTTP_PORT)
            .put("index.store.stats_refresh_interval", 0);
    node = NodeBuilder.nodeBuilder().settings(settingsBuilder).build();
    LOGGER.info("Elasticsearch node created");
    if (node != null) {
      node.start();
    }
  }

  @Before
  public void before() throws IOException {
    JestClient client = createClient();
    IndicesExists indicesExists = new IndicesExists.Builder(ES_INDEX).build();
    JestResult result1 = client.execute(indicesExists);
    if (result1.isSucceeded()) {
      //index exsits
      DeleteIndex deleteIndex = new DeleteIndex.Builder(ES_INDEX).build();
      JestResult result = client.execute(deleteIndex);
      if (!result.isSucceeded()) {
        throw new IOException("cannot delete index " + ES_INDEX);
      }
    }
  }

  private void sampleIndex(long nbDocs) throws Exception {
    Client client = node.client();
    final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefresh(true);

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    for (int i = 0; i < nbDocs; i++) {
      int index = i % scientists.length;
      String source = String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i);
      bulkRequestBuilder.add(client.prepareIndex(ES_INDEX, ES_TYPE, null).setSource
          (source));
    }
    final BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new IOException("Cannot insert samples in index " + ES_INDEX);
    }
  }

  private JestClient createClient() {
    HttpClientConfig.Builder builder =
        new HttpClientConfig.Builder("http://" + ES_IP + ":" + ES_HTTP_PORT).multiThreaded(true);
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(builder.build());
    return factory.getObject();
  }

  @Test
  public void testSizes() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    BoundedElasticsearchSource initialSource = read.getSource();
    assertEquals("Wrong estimated size", 46433, initialSource.getEstimatedSizeBytes
        (options));
    assertEquals("Wrong average doc size", 26, initialSource.getAverageDocSize());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    sampleIndex(NB_DOCS);
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project" };

    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS);
    output.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        String element = context.element();
        LOGGER.info(element);
      }
    }));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadWithQuery() throws Exception {
    sampleIndex(NB_DOCS);

    String query = "{\n"
        + "  \"query\": {\n"
        + "  \"match\" : {\n"
        + "    \"scientist\" : {\n"
        + "      \"query\" : \"Einstein\",\n"
        + "      \"type\" : \"boolean\"\n"
        + "    }\n"
        + "  }\n"
        + "  }\n"
        + "}";

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withQuery(
            query).withIndex(ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS / 10);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));

    pipeline.run();

    // upgrade
    node.client().admin().indices().upgrade(new UpgradeRequest()).actionGet();

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithBatchSizes() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    PDone collection = pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE).withBatchSize(NB_DOCS / 2).withBatchSizeMegaBytes(1));

    //TODO assert nb bundles == 2
    pipeline.run();

    // upgrade
    node.client().admin().indices().upgrade(new UpgradeRequest()).actionGet();

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  public void testSplitsWithBiggerDesiredBundleSizeThanShardSize() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    BoundedElasticsearchSource initialSource = read.getSource();
    int desiredBundleSizeBytes = 1073741824;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    int expectedNbSplits = 5;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits)
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    assertEquals("Wrong number of empty splits", expectedNbSplits, nonEmptySplits);
  }

  @Test
  public void testSplitsWithSmallerDesiredBundleSizeThanShardSize() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    BoundedElasticsearchSource initialSource = read.getSource();
    long desiredBundleSizeBytes = 4000;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    long expectedNbSplits = 7;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits)
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    assertEquals("Wrong number of empty splits", expectedNbSplits, nonEmptySplits);
  }

  @AfterClass
  public static void afterClass() {
    if (node != null) {
      node.close();
    }
  }

}
