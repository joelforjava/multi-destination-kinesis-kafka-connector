package com.amazon.kinesis.kafka.config;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClusterMappingTest {

    @Test
    void testGetStreamsAsMapIsCreatedAsExpected() throws Exception {
        String path = "sample_cluster_1.yaml";

        Optional<ClusterMapping> optional = ConfigParser.parse(path);
        Assert.assertTrue(optional.isPresent());
        ClusterMapping mapping = optional.get();
        List<DestinationStreamMapping> streams =  mapping.getStreams();
        Map<String, List<String>> streamMap = mapping.getStreamsAsMap();

        streams.forEach(stream -> {
            Assert.assertTrue(streamMap.keySet().contains(stream.getName()));
            Assert.assertTrue(streamMap.values().contains(stream.getDestinations()));
        });
    }
}
