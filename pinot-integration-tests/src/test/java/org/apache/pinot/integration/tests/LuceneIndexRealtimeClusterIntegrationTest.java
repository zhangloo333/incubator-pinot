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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
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
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.utils.KafkaStarterUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.tools.data.generator.AvroWriter;
import org.apache.pinot.tools.query.comparison.StarTreeQueryGenerator;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LuceneIndexRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
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

    startKafka();

    _schema = new Schema();
    _schema.setSchemaName(DEFAULT_TABLE_NAME);
    FieldSpec byteField = new DimensionFieldSpec();
    byteField.setDataType(DataType.BYTES);
    byteField.setDefaultNullValue("{}".getBytes());
    byteField.setObjectType("JSON");
    byteField.setName("headers");
    byteField.setSingleValueField(true);
    _schema.addField(byteField);

    FieldSpec timeField = new TimeFieldSpec();
    timeField.setDataType(DataType.LONG);
    ((TimeFieldSpec) timeField).setIncomingGranularitySpec(new TimeGranularitySpec(DataType.LONG,TimeUnit.DAYS,"daysSinceEpoch"));
    timeField.setName("daysSinceEpoch");
    timeField.setSingleValueField(true);
    _schema.addField(timeField);

    // Create the tables
    ArrayList<String> invertedIndexColumns = Lists.newArrayList("headers");
    ArrayList<String> noDictionaryColumns = Lists.newArrayList("headers");

    File avroFile = createAvroFile();
    _currentTable = DEFAULT_TABLE_NAME;
    addSchema(_schema);
    addRealtimeTable(getTableName(), true, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KafkaStarterUtils.DEFAULT_ZK_STR,
        getKafkaTopic(), getRealtimeSegmentFlushSize(), avroFile, null, null, DEFAULT_TABLE_NAME, null, null,
        getLoadMode(), null, invertedIndexColumns, null, noDictionaryColumns,
        getTaskConfig(), getStreamConsumerFactoryClassName());
    completeTableConfiguration();
    pushAvroIntoKafka(Lists.newArrayList(avroFile), getKafkaTopic(), Executors.newCachedThreadPool());
    Thread.currentThread().join();
    // Wait for all documents loaded
    waitForAllDocsLoaded(10_000);
  }

  @Override
  protected long getCountStarResult() {
    return TOTAL_DOCS;
  }
  
  protected File createAvroFile() throws Exception {
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
      record.put("daysSinceEpoch", 15825L);
      recordWriter.append(record);
    }
    recordWriter.close();

    return avroFile;

  }


  @Test
  public void testQueries() throws Exception {
    String pqlQuery =
        "Select count(*) from " + DEFAULT_TABLE_NAME + " WHERE headers matches('k1:\"value\"','')";
    JsonNode pinotResponse = postQuery(pqlQuery);
    //expect 1000
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
