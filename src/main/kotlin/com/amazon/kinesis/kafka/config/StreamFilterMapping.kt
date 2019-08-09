package com.amazon.kinesis.kafka.config

data class StreamFilterMapping(val sourceTopic: String = "",
                               val destinationStreamNames: List<String>? = null,
                               val keywords: List<String>? = null,
                               val startingPhrases: List<String>? = null)