package com.amazon.kinesis.kafka.config

class ClusterMapping {

    val clusterName: String = ""
    val streams: List<DestinationStreamMapping> = emptyList()

    val streamsAsMap: Map<String, List<String>?>
        get() = streams.associateBy( {it.name}, {it.destinations} )

    fun gatherStreamFilters(): Map<String, List<StreamFilterMapping>?> {
        return streams.associateBy( { it.name }, { it.filters })
    }
}
