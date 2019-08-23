package com.amazon.kinesis.kafka.config;

import org.apache.kafka.common.config.ConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class DestinationStreamMappingTest {

    @Test
    void testFiltersAreEmptyWhenNoneAreInYamlConfig() {
        String path = "sample_cluster_2_w_filters.yaml";

        ClusterMapping mapping = MappingConfigParser.parse(path)
                                             .orElseThrow(() -> new ConfigException(""));
        List<DestinationStreamMapping> streams =  mapping.getStreams();
        boolean emptyFilterListsExist = streams.stream().anyMatch(stream -> stream.getFilters().isEmpty());
        Assert.assertTrue(emptyFilterListsExist);
        long emptyFilterListCount = streams.stream().filter(stream -> stream.getFilters().isEmpty()).count();
        Assert.assertEquals(emptyFilterListCount, 2);
    }
}
