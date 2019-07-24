package com.amazon.kinesis.kafka.config;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
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
