package com.amazon.kinesis.kafka;

import java.util.*;

public class StreamMappings {

    private static final Map<String, List<String>> CLUSTER_1;
    static {
        CLUSTER_1 = new LinkedHashMap<>();
        CLUSTER_1.put("IMPORTANT.TOPIC", Arrays.asList("IMPORTANT-STREAM", "S3-IMPORTANT-STREAM"));
        CLUSTER_1.put("FASCINATING.TOPIC", Arrays.asList("FASCINATING-STREAM", "S3-FASCINATING-STREAM"));
        CLUSTER_1.put("METRICBEAT.TOPIC", Arrays.asList("METRICBEAT-STREAM", "S3-METRICBEAT-STREAM"));
        CLUSTER_1.put("LOGSTASH.TOPIC", Arrays.asList("LOGSTASH-STREAM", "S3-LOGSTASH-STREAM"));
        CLUSTER_1.put("RABBITMQ.TOPIC", Arrays.asList("RABBITMQ-STREAM", "S3-RABBITMQ-STREAM"));
    }

    private static final Map<String, List<String>> CLUSTER_2;
    static {
        CLUSTER_2 = new LinkedHashMap<>();
        CLUSTER_2.put("TEMPERATURES.TOPIC", Arrays.asList("TEMPERATURES-STREAM", "S3-TEMPERATURES-STREAM", "WEATHER-STREAM"));
        CLUSTER_2.put("BIOMETRICS.TOPIC", Arrays.asList("BIOMETRICS-STREAM", "S3-BIOMETRICS-STREAM"));
        CLUSTER_2.put("HURRICANES.TOPIC", Arrays.asList("HURRICANES-STREAM", "S3-HURRICANES-STREAM", "WEATHER-STREAM"));
    }

    private static final Map<String, List<String>> CLUSTER_3;
    static {
        CLUSTER_3 = new LinkedHashMap<>();
        CLUSTER_3.put("FUTURES.TOPIC", Arrays.asList("FUTURES-STREAM"));
        CLUSTER_3.put("GOLD.TOPIC", Arrays.asList("GOLD-STREAM", "S3-GOLD-STREAM", "COMMODITIES-STREAM"));
        CLUSTER_3.put("BITCOIN.TOPIC", Arrays.asList("BITCOIN-STREAM", "S3-BITCOIN-STREAM", "COMMODITIES-STREAM"));
        CLUSTER_3.put("ETHEREUM.TOPIC", Arrays.asList("ETHEREUM-STREAM", "COMMODITIES-STREAM"));
    }

    static Map<String, List<String>> lookup(String clusterName) {
        switch (clusterName.toUpperCase()) {
            case "CLUSTER_1":
                return CLUSTER_1;
            case "CLUSTER_2":
                return CLUSTER_2;
            case "CLUSTER_3":
                return CLUSTER_3;
            default:
                return Collections.emptyMap();
        }
    }
}
