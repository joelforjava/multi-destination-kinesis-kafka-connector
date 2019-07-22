package com.amazon.kinesis.kafka.config;

import com.amazon.kinesis.kafka.StreamMappings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConfigParserTest {

    @Test
    public void testParseCanLoadValidClusterMappingYaml() throws Exception {
        String path = "sample_cluster_1.yaml";

        Optional<ClusterMapping> mapping = ConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());

        ClusterMapping clusterMapping = mapping.get();
        Assert.assertEquals(clusterMapping.getClusterName(), "TEST_CLUSTER_1");
    }

    @Test
    void testParserCanLoadValidClusterMappingYamlGivenFullFileUrl() throws Exception {
        String configDirectory = loadConfigDirectory();
        String path = configDirectory + File.separator + "cluster_1" + File.separator + "streamMapping.yaml";

        Optional<ClusterMapping> mapping = ConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());

        ClusterMapping clusterMapping = mapping.get();
        Assert.assertEquals(clusterMapping.getClusterName(), "CLUSTER_1");

    }

    // This is used to ensure our new YAML files have the same topics, etc. Once StreamMappings.java is removed, these tests will be removed also
    @Test
    void testYamlConfigIsEquivalentToStreamMappingsConfig() throws Exception {
        List<String> clusterNames = Arrays.asList("cluster_1", "cluster_2", "cluster_3");
        clusterNames.forEach(this::testYamlConfigIsEquivalentToStreamMappingsConfig);
    }

    private void testYamlConfigIsEquivalentToStreamMappingsConfig(String clusterName) {
        String configDirectory = loadConfigDirectory();
        String path = configDirectory + File.separator + clusterName + File.separator + "streamMapping.yaml";

        Optional<ClusterMapping> mapping = ConfigParser.parse(path);
        Assert.assertTrue(mapping.isPresent());
        ClusterMapping clusterMapping = mapping.get();

        Map<String, List<String>> streamsFromYaml = clusterMapping.getStreamsAsMap();
        Map<String, List<String>> streamsFromCode = StreamMappings.lookup(clusterName);
        Assert.assertEquals(streamsFromYaml, streamsFromCode);

    }

    @Test
    void testParserWillReturnNullForInvalidClusterMappingYaml() throws Exception {
        String path = "badCluster.yaml";
        Optional<ClusterMapping> mapping = ConfigParser.parse(path);

        Assert.assertFalse(mapping.isPresent());
    }


    private String loadConfigDirectory() {
        return System.getProperty("configDirectory");
    }
}
