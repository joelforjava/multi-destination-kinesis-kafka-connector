package com.amazon.kinesis.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FirehoseSinkTaskTest {


    private static final int PARTITION = 11;

    private Map<String, String> createCommonProps() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(FirehoseSinkConnector.BATCH_SIZE, "15");
        props.put(FirehoseSinkConnector.BATCH_SIZE_IN_BYTES, "1024");
        props.put(FirehoseSinkConnector.REGION, "us-east-16");
        props.put(FirehoseSinkConnector.BATCH, "true");

        return props;
    }

    private Schema createSchema() {
        return SchemaBuilder.bytes().build();
    }

    @Test
    public void testMessagesSentToExpectedFirehosesWhenUsingMappingFile() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();
        Map<String, String> props = createCommonProps();
        props.put(FirehoseSinkConnector.MAPPING_FILE, "sample_cluster_1.yaml");

        task.start(props, mockClient);

        final String topicName = "METRICBEAT.TOPIC";
        final String[] expectedataStreamNames = { "METRICBEAT-STREAM", "S3-METRICBEAT-STREAM"};

        Collection<SinkRecord> records = new ArrayList<>();
        Schema schema = createSchema();
        String key = "theKey";
        int offset = 0;
        String message = "{\"message\":\"Hey I'm a metricbeat message!\"}";
        SinkRecord sinkRecord = new SinkRecord(topicName, PARTITION, Schema.BYTES_SCHEMA, key, schema, message.getBytes(), offset);
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

    @Test(expectedExceptions = ConfigException.class, expectedExceptionsMessageRegExp = "Connector cannot start.*")
    public void testNoClusterNameInConfigurationResultsInException() {
        FirehoseSinkTask task = new FirehoseSinkTask();
        MockFirehoseClient mockClient = new MockFirehoseClient();

        Map<String, String> props = createCommonProps();
        task.start(props, mockClient);
    }
}
