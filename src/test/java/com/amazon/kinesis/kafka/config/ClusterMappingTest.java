package com.amazon.kinesis.kafka.config;

import org.apache.kafka.common.config.ConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClusterMappingTest {

    @Test
    void testGetStreamsAsMapIsCreatedAsExpected() throws Exception {
        String path = "sample_cluster_1.yaml";

        Optional<ClusterMapping> optional = MappingConfigParser.parse(path);
        Assert.assertTrue(optional.isPresent());
        ClusterMapping mapping = optional.get();
        List<DestinationStreamMapping> streams =  mapping.getStreams();
        Map<String, List<String>> streamMap = mapping.getStreamsAsMap();

        streams.forEach(stream -> {
            Assert.assertTrue(streamMap.containsKey(stream.getName()));
            Assert.assertTrue(streamMap.containsValue(stream.getDestinations()));
        });
    }

    @Test
    void testGatherStreamFiltersCollectsAsExpected() {
        String path = "sample_cluster_2_w_filters.yaml";

        ClusterMapping mapping = MappingConfigParser.parse(path).orElseThrow(() -> new ConfigException(""));
        Map<String, List<StreamFilterMapping>> filtersMap = mapping.gatherStreamFilters();
        List<DestinationStreamMapping> streams =  mapping.getStreams();
        DestinationStreamMapping destinationStreamMapping = streams.stream()
                                                                .filter(stream -> "BIOMETRICS.TOPIC".equals(stream.getName()))
                                                                .findFirst()
                                                                .orElseThrow(() -> new ConfigException("Invalid Configuration"));
        Assert.assertTrue(filtersMap.get("BIOMETRICS.TOPIC").size() > 0);
        Assert.assertEquals(filtersMap.get("BIOMETRICS.TOPIC"), destinationStreamMapping.getFilters());
    }
}
