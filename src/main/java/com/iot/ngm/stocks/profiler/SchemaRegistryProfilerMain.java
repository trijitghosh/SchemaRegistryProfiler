package com.iot.ngm.stocks.profiler;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.iot.ngm.stocks.dtos.Stock;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class SchemaRegistryProfilerMain {

    private static Logger log;
    private static AppConfig appConfig;
    private static Gson gson;

    public SchemaRegistryProfilerMain() {
        log = LoggerFactory.getLogger(SchemaRegistryProfilerMain.class.getSimpleName());
        appConfig = new AppConfig(ConfigFactory.load());
        gson = new Gson();
    }

    public static void main(String[] args) {
        SchemaRegistryProfilerMain srp = new SchemaRegistryProfilerMain();
        SpecificAvroSerde<Stock> stockSerde = new SpecificAvroSerde<>();
        stockSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        KafkaStreams kStreams = new KafkaStreams(srp.createTopology(stockSerde), srp.getStreamsConfig());
        //kStreams.cleanUp();
        kStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }

    public Properties getStreamsConfig(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        return config;
    }

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

    // check if record is in json format and has expected fields
    public boolean isValidRecord(String payload) {
        if(isValidJson(payload))
            return parseJsonToStock(payload) != null;
        return false;
    }

    // check if string is in json format
    public boolean isValidJson(String payload) {
        try {
            gson.fromJson(payload, Object.class);
            return true;
        } catch(JsonSyntaxException e) {
            return false;
        }
    }

    // parse json string to Stock
    public Stock parseJsonToStock(String payload){
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
        }
        catch (IllegalStateException | NullPointerException e){
            return null;
        }
    }
}
