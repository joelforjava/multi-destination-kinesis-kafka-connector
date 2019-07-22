package com.amazon.kinesis.kafka.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterMapping {

    private String clusterName;
    private List<DestinationStreamMapping> streams;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<DestinationStreamMapping> getStreams() {
        return streams;
    }

    public void setStreams(List<DestinationStreamMapping> streams) {
        this.streams = streams;
    }

    public Map<String, List<String>> getStreamsAsMap() {
        if (streams == null) {
            return Collections.emptyMap();
        }

        return streams.stream()
                .collect(Collectors.toMap(DestinationStreamMapping::getName, DestinationStreamMapping::getDestinations));
    }

}
