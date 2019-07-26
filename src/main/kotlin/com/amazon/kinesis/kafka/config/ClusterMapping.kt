package com.amazon.kinesis.kafka.config

class ClusterMapping {

    var clusterName: String? = null
    var streams: List<DestinationStreamMapping>? = null

    val streamsAsMap: Map<String, List<String>?>
        get() = if (streams == null) {
            emptyMap()
        } else {
            // TODO - I don't really like this, but it'll have to do in order
            // for this to work with SnakeYAML. Research other YAML parsers.
            streams!!.associateBy( {it.name!!}, {it.destinations} )
        }

}
