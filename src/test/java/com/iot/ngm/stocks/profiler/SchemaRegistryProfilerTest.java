package com.iot.ngm.stocks.profiler;

import com.google.gson.JsonObject;
import com.iot.ngm.stocks.dtos.Stock;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.*;

public class SchemaRegistryProfilerTest {

    SchemaRegistryProfilerMain schemaRegistryProfiler;
    MockSchemaRegistryClient mockSRClient;
    TopologyTestDriver testDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Stock> outputTopic;

    @Before
    public void setupTopologyTestDriver(){
        schemaRegistryProfiler = new SchemaRegistryProfilerMain();
        mockSRClient = new MockSchemaRegistryClient();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:5678");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        SpecificAvroSerde<Stock> stockSerde = new SpecificAvroSerde<>(mockSRClient);
        stockSerde.configure(Collections.singletonMap("schema.registry.url", "mock://dummy:5678"), false);
        testDriver = new TopologyTestDriver(schemaRegistryProfiler.createTopology(stockSerde), config);
        inputTopic = testDriver.createInputTopic("time_series_intraday_prices", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic("company_stock", new StringDeserializer(), stockSerde.deserializer());
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    @Test
    public void streamTest() {
        inputTopic.pipeInput(new TestRecord<>(createDummyEvent("IBM").toString()));
        TestRecord<String, Stock> evt1 = outputTopic.readRecord();
        assertEquals(evt1.getKey(), "IBM");
        assertEquals(evt1.getValue().getTime(), Instant.parse("2022-02-16T19:17:00Z"));
        assertEquals(evt1.getValue().getOpen(), 129.12f, 0.0f);
        assertEquals(evt1.getValue().getHigh(), 129.12f, 0.0f);
        assertEquals(evt1.getValue().getLow(), 129.09f, 0.0f);
        assertEquals(evt1.getValue().getClose(), 129.09f, 0.0f);
        assertEquals(evt1.getValue().getSymbol(), "IBM");

        inputTopic.pipeInput(new TestRecord<>(createDummyEvent("Amazon").toString()));
        TestRecord<String, Stock> evt2 = outputTopic.readRecord();
        assertEquals(evt2.getKey(), "Amazon");
        assertEquals(evt2.getValue().getTime(), Instant.parse("2022-02-16T19:17:00Z"));
        assertEquals(evt2.getValue().getOpen(), 129.12f, 0.0f);
        assertEquals(evt2.getValue().getHigh(), 129.12f, 0.0f);
        assertEquals(evt2.getValue().getLow(), 129.09f, 0.0f);
        assertEquals(evt2.getValue().getClose(), 129.09f, 0.0f);
        assertEquals(evt2.getValue().getSymbol(), "Amazon");
    }

    @Test
    public void validRecordTest() {
        String invalidJson = "1}abc";
        assertFalse(schemaRegistryProfiler.isValidRecord(invalidJson));

        String noTimeRecord = removeProperty(createDummyEvent(), "time");
        assertFalse(schemaRegistryProfiler.isValidRecord(noTimeRecord));
        String noOpenRecord = removeProperty(createDummyEvent(), "open");
        assertFalse(schemaRegistryProfiler.isValidRecord(noOpenRecord));
        String noHighRecord = removeProperty(createDummyEvent(), "high");
        assertFalse(schemaRegistryProfiler.isValidRecord(noHighRecord));
        String noLowRecord = removeProperty(createDummyEvent(), "low");
        assertFalse(schemaRegistryProfiler.isValidRecord(noLowRecord));
        String noCloseRecord = removeProperty(createDummyEvent(), "close");
        assertFalse(schemaRegistryProfiler.isValidRecord(noCloseRecord));
        String noSymbolRecord = removeProperty(createDummyEvent(), "symbol");
        assertFalse(schemaRegistryProfiler.isValidRecord(noSymbolRecord));

        String dummyEvt = createDummyEvent().toString();
        assertTrue(schemaRegistryProfiler.isValidRecord(dummyEvt));
    }

    @Test
    public void validJsonTest() {
        String invalidJson = "1}abc";
        assertFalse(schemaRegistryProfiler.isValidJson(invalidJson));
        String dummyEvt = createDummyEvent().toString();
        assertTrue(schemaRegistryProfiler.isValidJson(dummyEvt));
    }

    @Test
    public void jsonParserTest() {
        String dummyEvt = createDummyEvent().toString();
        Stock s = schemaRegistryProfiler.parseJsonToStock(dummyEvt);
        assertEquals(s.getTime(), Instant.parse("2022-02-16T19:17:00Z"));
        assertEquals(s.getOpen(), 129.12f, 0.0f);
        assertEquals(s.getHigh(), 129.12f, 0.0f);
        assertEquals(s.getLow(), 129.09f, 0.0f);
        assertEquals(s.getClose(), 129.09f, 0.0f);
        assertEquals(s.getSymbol(), "IBM");
    }

    /*
    Create JSON record compliant with the data received by Kafka, for symbol IBM.
     */
    private JsonObject createDummyEvent() {
        return createDummyEvent("IBM");
    }

    /*
    Create JSON record compliant with the data received by Kafka, with variable symbol as input.
    E.g.: {"time": "2022-02-16 19:17:00", "open": 129.12, "high": 129.12, "low": 129.09, "close": 129.09, "symbol": "IBM"}
     */
    private JsonObject createDummyEvent(String symbol) {
        JsonObject dummyEvt = new JsonObject();
        dummyEvt.addProperty("time", "2022-02-16 19:17:00");
        dummyEvt.addProperty("open", 129.12f);
        dummyEvt.addProperty("high", 129.12f);
        dummyEvt.addProperty("low", 129.09f);
        dummyEvt.addProperty("close", 129.09f);
        dummyEvt.addProperty("symbol", symbol);
        return dummyEvt;
    }

    /*
    Remove property from JSON object and return the remaining object as a string.
     */
    private String removeProperty(JsonObject json, String property) {
        json.remove(property);
        return json.toString();
    }
}
