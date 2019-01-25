/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.tools.data.generator.AvroWriter;
import org.apache.pinot.tools.query.comparison.StarTreeQueryGenerator;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class LuceneIndexClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected static final String DEFAULT_TABLE_NAME = "myTable";
  static final long TOTAL_DOCS = 1_000L;

  protected Schema _schema;
  private StarTreeQueryGenerator _queryGenerator;
  private String _currentTable;

  @Nonnull
  @Override
  protected String getTableName() {
    return _currentTable;
  }

  @Nonnull
  @Override
  protected String getSchemaFileName() {
    return null;
  }

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(1);

    _schema = new Schema();
    FieldSpec jsonFieldSpec = new DimensionFieldSpec();
    jsonFieldSpec.setDataType(DataType.BYTES);
    jsonFieldSpec.setDefaultNullValue("{}".getBytes());
    jsonFieldSpec.setObjectType("JSON");
    jsonFieldSpec.setName("headers");
    jsonFieldSpec.setSingleValueField(true);
    _schema.addField(jsonFieldSpec);

    // Create the tables
    ArrayList<String> invertedIndexColumns = Lists.newArrayList("headers");
    addOfflineTable(DEFAULT_TABLE_NAME, null, null, null, null, null, SegmentVersion.v1,
        invertedIndexColumns, null, null);

    setUpSegmentsAndQueryGenerator();

    // Wait for all documents loaded
    _currentTable = DEFAULT_TABLE_NAME;
    waitForAllDocsLoaded(10_000);
  }

  @Override
  protected long getCountStarResult() {
    return TOTAL_DOCS;
  }
  
  protected void setUpSegmentsAndQueryGenerator() throws Exception {
    org.apache.avro.Schema avroSchema = AvroWriter.getAvroSchema(_schema);
    DataFileWriter recordWriter =
        new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(avroSchema));
    String parent = "/tmp/luceneTest";
    File avroFile = new File(parent, "part-" + 0 + ".avro");
    avroFile.getParentFile().mkdirs();
    recordWriter.create(avroSchema, avroFile);
    ObjectMapper mapper = new ObjectMapper();
    for (int i = 0; i < TOTAL_DOCS; i++) {
      ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
      objectNode.put("k1", "value" + i);
      objectNode.put("k2", "value" + i);
      String json = mapper.writeValueAsString(objectNode);
      GenericData.Record record = new GenericData.Record(avroSchema);
      ByteBuffer byteBuffer = ByteBuffer.wrap(json.getBytes());
      record.put("headers", byteBuffer);
      recordWriter.append(record);
    }
    recordWriter.close();

    // Unpack the Avro files
    List<File> avroFiles = Lists.newArrayList(avroFile);

    // Create and upload segments without star tree indexes from Avro data
    createAndUploadSegments(avroFiles, DEFAULT_TABLE_NAME, false);

  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName,
      boolean createStarTreeIndex) throws Exception {
    System.out.println("SEGMENT_DIR" + _segmentDir);
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName,
        createStarTreeIndex, null, null, _schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(_tarDir);
  }

  @Test
  public void testQueries() throws Exception {
    String pqlQuery =
        "Select count(*) from " + DEFAULT_TABLE_NAME + " WHERE headers matches('k1:\"value\\-1\"','')";
    JsonNode pinotResponse = postQuery(pqlQuery);
    System.out.println(pinotResponse);
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

}
