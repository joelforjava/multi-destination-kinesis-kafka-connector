package com.amazon.kinesis.kafka.config;

import org.apache.kafka.common.config.ConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FirehoseSinkConnectorConfigTest {

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Missing required configuration.*")
    public void testAttemptingToCreateConfigWithEmptyMapResultsInAnError() {
        Map<String, String> props = new HashMap<>();
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
        Assert.assertNotNull(config);
    }

    @Test
    public void testAttemptingToCreateConfigWithOnlyMappingFileIsValid() {
        Map<String, String> props = new HashMap<>();
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
        Assert.assertNotNull(config);
    }

    @Test
    public void testCreatingBareMinimumPropertiesForConfigResultsInExpectedDefaults() {
        Map<String, String> props = new HashMap<>();
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
        Assert.assertEquals(
                Integer.valueOf(FirehoseSinkConnectorConfig.DEFAULT_BATCH_SIZE_IN_BYTES),
                config.getInt(FirehoseSinkConnectorConfig.BATCH_SIZE_IN_BYTES_CONFIG));
        Assert.assertEquals(
                Integer.valueOf(FirehoseSinkConnectorConfig.MAX_BATCH_SIZE),
                config.getInt(FirehoseSinkConnectorConfig.BATCH_SIZE_CONFIG));

    }

    @Test
    public void testDefaultsCanBeOverridden() {
        Map<String, String> props = new HashMap<>();
        String batchSize = "300";
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");
        props.put(FirehoseSinkConnectorConfig.BATCH_SIZE_CONFIG, batchSize);
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
        Assert.assertEquals(
                Integer.valueOf(batchSize),
                config.getInt(FirehoseSinkConnectorConfig.BATCH_SIZE_CONFIG));

    }

    // In other words, properties like 'topics' or 'topics.regex' are validated by another class
    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Unknown configuration.*")
    public void testConfigOnlyRecognizesDefinedProperties() {
        Map<String, String> props = new HashMap<>();
        String topics = "TOPIC-1,TOPIC-2,TOPIC-3";
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG, topics);
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
        Assert.assertNotNull(config);
        Assert.assertEquals(
                Arrays.asList(topics.split(",")),
                config.getList(FirehoseSinkConnectorConfig.TOPICS_CONFIG));
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Invalid value.*")
    public void testPropertyWithInvalidMaxValueWillBeRejected() {
        Map<String, String> props = new HashMap<>();
        String batchSize = "5000";
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");
        props.put(FirehoseSinkConnectorConfig.BATCH_SIZE_CONFIG, batchSize);
        FirehoseSinkConnectorConfig config = new FirehoseSinkConnectorConfig(props);
    }
}
