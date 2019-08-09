package com.amazon.kinesis.kafka.config

data class DestinationStreamMapping(val name: String = "",
                                    val destinations: List<String>? = null,
                                    val filters: List<StreamFilterMapping> = emptyList()) {
    fun getFiltersAsMap(): Map<String, List<StreamFilterMapping>?> {
        return mapOf( name to filters)
    }
}
