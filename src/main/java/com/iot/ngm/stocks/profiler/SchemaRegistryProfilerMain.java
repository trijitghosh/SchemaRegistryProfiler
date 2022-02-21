package com.iot.ngm.stocks.profiler;

import com.iot.ngm.stocks.dtos.Stock;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class SchemaRegistryProfilerMain {

    private static Logger log;
    private static AppConfig appConfig;
    private static KafkaConsumer<String, String> kConsumer;
    private static KafkaProducer<String, Stock> kProducer;
    //String dummyEvent = "{\"time\": \"2022-02-16 19:17:00\", \"open\": 129.12, \"high\": 129.12, \"low\": 129.09, \"close\": 129.09, \"volume\": 402, \"symbol\": \"IBM\"}";
    //{"time": "2022-02-16 19:17:00", "open": 129.12, "high": 129.12, "low": 129.09, "close": 129.09, "volume": 402, "symbol": "IBM"}

    public static void main(String[] args) {
        log = LoggerFactory.getLogger(SchemaRegistryProfilerMain.class.getSimpleName());
        appConfig = new AppConfig(ConfigFactory.load());
        kConsumer = createKafkaConsumer();
        kProducer = createKafkaProducer();
        kConsumer.subscribe(Collections.singleton(appConfig.getRawTopicName()));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kConsumer.close();
            kProducer.close();
        }));
        work();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, appConfig.getConsumerGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private static KafkaProducer<String, Stock> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());
        return new KafkaProducer<>(properties);
    }

    private static void work() {
        String targetTopic = appConfig.getParsedTopicName();
        log.info("Started polling for data at [" + appConfig.getRawTopicName() + "] topic.");
        while(true){
            ConsumerRecords<String, String> records = kConsumer.poll(Duration.ofMillis(appConfig.getConsumerPollInterval()));
            for (ConsumerRecord<String, String> record : records){
                String v = record.value();
                log.info("Received stock from [" + appConfig.getRawTopicName() + "] topic: " + v);
                Stock stock = parseJsonToStock(v);
                log.info("Parsed stock to avro: " + stock);
                if(stock!=null) {
                    kProducer.send(new ProducerRecord<>(targetTopic, stock.getSymbol(), stock));
                    log.info("Sent stock to [" + appConfig.getParsedTopicName() + "] topic.");
                }
            }
            kConsumer.commitSync();
        }
    }

    private static Stock parseJsonToStock(String payload){
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
        catch (NullPointerException e){
            return null;
        }
    }
}
