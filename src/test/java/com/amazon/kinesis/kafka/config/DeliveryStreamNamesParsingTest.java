package com.amazon.kinesis.kafka.config;

import com.amazon.kinesis.kafka.StreamMappings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This was an alternate approach considered before implementing the YAML configurations.
 * We would parse a property named 'deliveryStreamNames' and use [TOPIC]:[STREAM-1]:[STREAM-2],[TOPIC-2]:[STREAM-3], etc.
 * However, this setup is quite error prone and it's easy to use a comma where a colon should be and vice-versa.
 */
public class DeliveryStreamNamesParsingTest {


    @Test
    void testStreamNamesParsedFromDeliveryStreamNamesAsExpected() throws Exception {
        String deliveryStreamName = "FUTURES.TOPIC:FUTURES-STREAM,GOLD.TOPIC:GOLD-STREAM:S3-GOLD-STREAM:COMMODITIES-STREAM,BITCOIN.TOPIC:BITCOIN-STREAM:S3-BITCOIN-STREAM:COMMODITIES-STREAM,ETHEREUM.TOPIC:ETHEREUM-STREAM:COMMODITIES-STREAM";

        Map<String, List<String>> parsed = parseDeliveryStreamNames(deliveryStreamName);
        Map<String, List<String>> streamsFromCode = StreamMappings.lookup("cluster_3");
        Assert.assertEquals(parsed, streamsFromCode);

    }

    private Map<String, List<String>> parseDeliveryStreamNames(String deliveryStringNames) {
        Map<String, List<String>> m = new LinkedHashMap<>();

        String[] topicMappings = deliveryStringNames.split(",");

        for (String mapping : topicMappings) {
            String[] parsed = mapping.split(":");
            String topic = parsed[0];
            String[] streams = Arrays.copyOfRange(parsed, 1, parsed.length);
            m.put(topic, Arrays.asList(streams));
        }

        return m;
    }

}
