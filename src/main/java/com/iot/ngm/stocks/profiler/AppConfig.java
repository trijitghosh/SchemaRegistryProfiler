package com.iot.ngm.stocks.profiler;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String rawTopicName;
    private final String parsedTopicName;
    private final String applicationId;
    private final long consumerPollInterval;
    private final String timestampFormat;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.rawTopicName = config.getString("kafka.raw.topic.name");
        this.parsedTopicName = config.getString("kafka.parsed.topic.name");
        this.applicationId = config.getString("app.application.id");
        this.consumerPollInterval = config.getLong("app.consumer.poll.interval");
        this.timestampFormat = config.getString("app.timestamp.format");
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getRawTopicName() {
        return rawTopicName;
    }

    public String getParsedTopicName() {
        return parsedTopicName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public long getConsumerPollInterval() {
        return consumerPollInterval;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

}
