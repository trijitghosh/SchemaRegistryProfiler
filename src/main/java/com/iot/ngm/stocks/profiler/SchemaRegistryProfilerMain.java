package com.iot.ngm.stocks.profiler;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.iot.ngm.stocks.dtos.Stock;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams application that reads json data from given input topic,
 * parses it to avro and sends it to a given output topic.
 */
public class SchemaRegistryProfilerMain {

  private static Logger log;
  private static AppConfig appConfig;
  private static Gson gson;

  /**
   * Constructor.
   */
  public SchemaRegistryProfilerMain() {
    log = LoggerFactory.getLogger(SchemaRegistryProfilerMain.class.getSimpleName());
    appConfig = new AppConfig(ConfigFactory.load());
    gson = new Gson();
  }

  /**
   * Main method. Sets Serde, creates Kafka Streams topology and config, and starts Kafka Streams.
   *
   * @param args input arguments (unused)
   */
  public static void main(String[] args) {
    SchemaRegistryProfilerMain srp = new SchemaRegistryProfilerMain();
    SpecificAvroSerde<Stock> stockSerde = new SpecificAvroSerde<>();
    stockSerde.configure(
        Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            appConfig.getSchemaRegistryUrl()), false);
    KafkaStreams kafkaStreams =
        new KafkaStreams(srp.createTopology(stockSerde), srp.getStreamsConfig());
    //kStreams.cleanUp();
    kafkaStreams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }

  /**
   * Sets Kafka Streams properties.
   *
   * @return stream config properties
   */
  public Properties getStreamsConfig() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        appConfig.getSchemaRegistryUrl());
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
    return config;
  }

  /**
   * Creates the topology for Kafka Streams.
   * 1: consumes data from input topic
   * 2: drops malformed records
   * 3: selects symbol as the KStream key
   * 4: parses values from a JSON string to Stock (avro)
   * 5: appends records to application log
   * 6: produces data to output topic
   *
   * @param stockSerde Stock Serde.
   * @return stream topology
   */
  public Topology createTopology(SpecificAvroSerde<Stock> stockSerde) {
    StreamsBuilder builder = new StreamsBuilder();
    Serdes.StringSerde stringSerde = new Serdes.StringSerde();
    builder.stream(appConfig.getRawTopicName(), Consumed.with(stringSerde, stringSerde))
        // drop malformed records
        .filter((k, v) -> isValidRecord(v))
        // select symbol as key
        .selectKey((k, v) -> Objects.requireNonNull(parseJsonToStock(v)).getSymbol())
        // parse values to avro
        .mapValues(this::parseJsonToStock)
        // append record to application log
        .peek((k, stock) -> log.info("Parsed record = " + stock))
        // write to output topic
        .to(appConfig.getParsedTopicName(), Produced.with(stringSerde, stockSerde));
    return builder.build();
  }

  /**
   * Checks if record is in JSON format and has expected fields for a Stock record.
   *
   * @param payload record string to validate.
   * @return true, if record is valid; false, otherwise
   */
  public boolean isValidRecord(String payload) {
    if (isValidJson(payload)) {
      return parseJsonToStock(payload) != null;
    }
    return false;
  }

  /**
   * Checks if string is in a valid JSON format.
   *
   * @param payload JSON string to validate.
   * @return true, if string is valid; false, otherwise
   */
  public boolean isValidJson(String payload) {
    try {
      gson.fromJson(payload, Object.class);
      return true;
    } catch (JsonSyntaxException e) {
      return false;
    }
  }

  /**
   * Parses a JSON string to Stock.
   *
   * @param payload JSON string to parse.
   * @return resulting stock
   */
  public Stock parseJsonToStock(String payload) {
    final DateTimeFormatter formatter = DateTimeFormatter
        .ofPattern(appConfig.getTimestampFormat())
        .withZone(ZoneId.systemDefault());
    try {
      JsonObject json = JsonParser.parseString(payload).getAsJsonObject();
      return new Stock(Instant.from(formatter.parse(json.get("time").getAsString())),
          json.get("open").getAsFloat(),
          json.get("high").getAsFloat(),
          json.get("low").getAsFloat(),
          json.get("close").getAsFloat(),
          json.get("symbol").getAsString());
    } catch (IllegalStateException | NullPointerException e) {
      return null;
    }
  }

}
