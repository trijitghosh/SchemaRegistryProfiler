package com.iot.ngm.stocks.profiler;

import com.typesafe.config.Config;

/**
 * Application configuration properties.
 */
public class AppConfig {

  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final String rawTopicName;
  private final String parsedTopicName;
  private final String applicationId;
  private final String timestampFormat;

  /**
   * Constructor.
   *
   * @param config config file
   */
  public AppConfig(Config config) {
    this.bootstrapServers = config.getString("kafka.bootstrap.servers");
    this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
    this.rawTopicName = config.getString("kafka.raw.topic.name");
    this.parsedTopicName = config.getString("kafka.parsed.topic.name");
    this.applicationId = config.getString("app.application.id");
    this.timestampFormat = config.getString("app.timestamp.format");
  }

  /**
   * Get kafka.bootstrap.servers property
   *
   * @return bootstrap servers property
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * Get kafka.schema.registry.url property
   *
   * @return schema registry url property
   */
  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  /**
   * Get kafka.raw.topic.name property
   *
   * @return raw (input) topic name property
   */
  public String getRawTopicName() {
    return rawTopicName;
  }

  /**
   * Get kafka.parsed.topic.name property
   *
   * @return parsed (output) topic name property
   */
  public String getParsedTopicName() {
    return parsedTopicName;
  }

  /**
   * Get app.application.id property
   *
   * @return application id (for Kafka Streams) property
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Get app.timestamp.format property
   *
   * @return timestamp format (for timestamp fields in JSON input strings) property
   */
  public String getTimestampFormat() {
    return timestampFormat;
  }

}
