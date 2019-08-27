package com.amazon.kinesis.kafka;

import com.amazon.kinesis.kafka.config.FirehoseSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;

public class FirehoseSinkTaskTest {


    private static final int PARTITION = 11;

    private Map<String, String> createCommonProps() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(FirehoseSinkConnectorConfig.BATCH_SIZE_CONFIG, "3");
        props.put(FirehoseSinkConnectorConfig.BATCH_SIZE_IN_BYTES_CONFIG, "128");
        props.put(FirehoseSinkConnectorConfig.REGION_CONFIG, "us-east-1");
        props.put(FirehoseSinkConnectorConfig.BATCH_CONFIG, "true");

        return props;
    }

    private Schema createSchema() {
        return SchemaBuilder.bytes().build();
    }

    @Test
    public void testMessagesSentToExpectedFirehosesInBatchModeWhenUsingMappingFile() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC,RABBITMQ.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        props.put(FirehoseSinkConnectorConfig.BATCH_CONFIG, "true");

        task.start(props, mockClient);

        final String topicName = "METRICBEAT.TOPIC";
        final String[] expectedataStreamNames = { "METRICBEAT-STREAM", "S3-METRICBEAT-STREAM" };

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"Hey I'm a metricbeat message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
        records.add(sinkRecord);
        records.add(sinkRecord);
        records.add(sinkRecord);
        task.put(records);

        List<String> deliveryStreamNames = mockClient.getDeliveryStreamNames();
        Assert.assertTrue(deliveryStreamNames.containsAll(Arrays.asList(expectedataStreamNames)));

        List<String> s3DeliveryStreamNames = deliveryStreamNames.stream()
                .filter(name -> expectedataStreamNames[1].equals(name))
                .collect(Collectors.toList());
        List<String> nonS3DeliveryStreamNames = deliveryStreamNames.stream()
                .filter(name -> expectedataStreamNames[0].equals(name))
                .collect(Collectors.toList());

        Assert.assertEquals(s3DeliveryStreamNames.size(), 1);
        Assert.assertEquals(nonS3DeliveryStreamNames.size(), 1);
    }

    @Test
    public void testMessagesSentToExpectedFirehosesModeWhenUsingMappingFile() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC,RABBITMQ.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        props.put(FirehoseSinkConnectorConfig.BATCH_CONFIG, "false");

        task.start(props, mockClient);

        final String topicName = "METRICBEAT.TOPIC";
        final String[] expectedataStreamNames = { "METRICBEAT-STREAM", "S3-METRICBEAT-STREAM" };

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"Hey I'm a metricbeat message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
        records.add(sinkRecord);
        records.add(sinkRecord);
        records.add(sinkRecord);
        task.put(records);

        List<String> deliveryStreamNames = mockClient.getDeliveryStreamNames();
        Assert.assertTrue(deliveryStreamNames.containsAll(Arrays.asList(expectedataStreamNames)));

        List<String> s3DeliveryStreamNames = deliveryStreamNames.stream()
                .filter(name -> expectedataStreamNames[1].equals(name))
                .collect(Collectors.toList());
        List<String> nonS3DeliveryStreamNames = deliveryStreamNames.stream()
                .filter(name -> expectedataStreamNames[0].equals(name))
                .collect(Collectors.toList());

        // Notice 3 instead of 1. The mock will have 3 instances of putRecord being called
        // which will add the topic name 3 times each.
        Assert.assertEquals(s3DeliveryStreamNames.size(), 3);
        Assert.assertEquals(nonS3DeliveryStreamNames.size(), 3);
    }

    @Test
    public void testFilteredMessagesWithKeywordsGoToAdditionalFirehoses() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "TEMPERATURES.TOPIC,BIOMETRICS.TOPIC,HURRICANES.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");

        task.start(props, mockClient);

        final String topicName = "BIOMETRICS.TOPIC";
        final String[] expectedataStreamNames = { "BIOMETRICS-STREAM", "S3-BIOMETRICS-STREAM", "BLOODPRESSURE-STREAM" };

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"Hey I'm a Blood pressure message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
        records.add(sinkRecord);
        task.put(records);

        List<String> deliveryStreamNames = mockClient.getDeliveryStreamNames();
        Assert.assertTrue(deliveryStreamNames.containsAll(Arrays.asList(expectedataStreamNames)));
    }

    @Test
    public void testFilteredMessagesWithStartingPhrasesGoToAdditionalFirehoses() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "TEMPERATURES.TOPIC,BIOMETRICS.TOPIC,HURRICANES.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");

        task.start(props, mockClient);

        final String topicName = "BIOMETRICS.TOPIC";
        final String[] expectedataStreamNames = { "BIOMETRICS-STREAM", "S3-BIOMETRICS-STREAM", "HEARTRATE-STREAM" };

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"Heart rate message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
        records.add(sinkRecord);
        task.put(records);

        List<String> deliveryStreamNames = mockClient.getDeliveryStreamNames();
        Assert.assertTrue(deliveryStreamNames.containsAll(Arrays.asList(expectedataStreamNames)));
    }

    @Test
    public void testMessagesWithoutFilterValuesDoNotGoToAdditionalFirehoses() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "TEMPERATURES.TOPIC,BIOMETRICS.TOPIC,HURRICANES.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");

        task.start(props, mockClient);

        final String topicName = "BIOMETRICS.TOPIC";

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"I am Heart rate message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
        records.add(sinkRecord);
        task.put(records);

        List<String> deliveryStreamNames = mockClient.getDeliveryStreamNames();
        Assert.assertFalse(deliveryStreamNames.contains("HEARTRATE-STREAM"));
    }

    @Test
    public void testStartingWithoutSendingCommonPropsWillStartSuccessfully() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = new HashMap<>();
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "TEMPERATURES.TOPIC,BIOMETRICS.TOPIC,HURRICANES.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_2_w_filters.yaml");

        task.start(props, mockClient);
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Missing required configuration.*")
    public void testNoMappingFileNameInConfigurationResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        task.start(props, mockClient);
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testNoTopicsInConfigurationResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        task.start(props, mockClient);
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testMoreTopicsInPropertiesThanMappingResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC,RABBITMQ.TOPIC,ANOTHER.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        task.start(props, mockClient);
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testMoreTopicsInMappingsThanPropertiesResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        task.start(props, mockClient);

    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testSendingToTopicNotInPropertiesResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnectorConfig.TOPICS_CONFIG, "IMPORTANT.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        task.start(props, mockClient);

    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testNoStreamsMappedForTopicResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        mockClient.setMockedActiveResponse("INACTIVE"); // anything besides 'ACTIVE' is an error

        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC,RABBITMQ.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "cluster_with_no_destinations.yaml");
        task.start(props, mockClient);
    }

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testInactiveDeliveryStreamsAtStartResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        mockClient.setMockedActiveResponse("INACTIVE"); // anything besides 'ACTIVE' is an error

        Map<String, String> props = createCommonProps();
        props.put(
                FirehoseSinkConnectorConfig.TOPICS_CONFIG,
                "IMPORTANT.TOPIC,FASCINATING.TOPIC,METRICBEAT.TOPIC,LOGSTASH.TOPIC,RABBITMQ.TOPIC");
        props.put(FirehoseSinkConnectorConfig.MAPPING_FILE_CONFIG, "sample_cluster_1.yaml");
        task.start(props, mockClient);
    }
}
