package com.amazon.kinesis.kafka.config;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MappingConfigParserTest {

    @Test
    public void testParseCanLoadValidClusterMappingYaml() throws Exception {
        String path = "sample_cluster_1.yaml";

        Optional<ClusterMapping> mapping = MappingConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());

        ClusterMapping clusterMapping = mapping.get();
        Assert.assertEquals(clusterMapping.getClusterName(), "TEST_CLUSTER_1");
    }

    @Test
    void testParserCanLoadValidClusterMappingYamlGivenFullFileUrl() throws Exception {
        String configDirectory = loadConfigDirectory();
        String path = configDirectory + File.separator + "cluster_1" + File.separator + "streamMapping.yaml";

        Optional<ClusterMapping> mapping = MappingConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());

        ClusterMapping clusterMapping = mapping.get();
        Assert.assertEquals(clusterMapping.getClusterName(), "CLUSTER_1");

    }

    @Test
    void testParserWillReturnNullForInvalidClusterMappingYaml() throws Exception {
        String path = "badCluster.yaml";
        Optional<ClusterMapping> mapping = MappingConfigParser.parse(path);

        Assert.assertFalse(mapping.isPresent());
    }

    @Test
    void testParserCanParseYamlWithFilterMapping() {
        String path = "sample_cluster_2_w_filters.yaml";
        Optional<ClusterMapping> mapping = MappingConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());

        ClusterMapping clusterMapping = mapping.get();
        Map<String, List<StreamFilterMapping>> filters = clusterMapping.gatherStreamFilters();
        Assert.assertEquals(filters.size(), 3); // One list per configured topic
        Assert.assertEquals(filters.get("BIOMETRICS.TOPIC").size(), 2);
        Assert.assertEquals(filters.get("TEMPERATURES.TOPIC").size(), 0);
    }

    private String loadConfigDirectory() {
        return System.getProperty("configDirectory");
    }
}
