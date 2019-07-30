package com.amazon.kinesis.kafka.config

data class StreamFilterMapping(var sourceTopic: String? = null,
                               var destinationStreamNames: List<String>? = null,
                               var keywords: List<String>? = null,
                               var startingPhrases: List<String>? = null)