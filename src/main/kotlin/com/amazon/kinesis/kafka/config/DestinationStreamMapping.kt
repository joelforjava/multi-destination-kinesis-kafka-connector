package com.amazon.kinesis.kafka.config

data class DestinationStreamMapping(var name: String? = null,
                                    var destinations: List<String>? = null,
                                    var filters: List<StreamFilterMapping>? = null) {
    fun getFiltersAsMap(): Map<String, List<StreamFilterMapping>?> {
        if (filters == null) {
            return emptyMap()
        }
        return mapOf( name!! to filters)
    }
}
