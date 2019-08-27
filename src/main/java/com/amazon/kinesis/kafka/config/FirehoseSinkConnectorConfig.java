package com.amazon.kinesis.kafka.config;

import com.amazon.kinesis.kafka.FirehoseSinkConnector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FirehoseSinkConnectorConfig extends AbstractConfig {

    // Mirrored here mostly to keep code uniform
    public static final String TOPICS_CONFIG = FirehoseSinkConnector.TOPICS_CONFIG;

    public static final String DELIVERY_STREAM_CONFIG = "deliveryStream";

    public static final String REGION_CONFIG = "region";

    public static final String BATCH_CONFIG = "batch";

    public static final String BATCH_SIZE_CONFIG = "batchSize";

    public static final String BATCH_SIZE_IN_BYTES_CONFIG = "batchSizeInBytes";

    public static final String MAPPING_FILE_CONFIG = "mappingFile";

    public static final int MAX_BATCH_SIZE = 500;

    public static final int DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE;

    public static final int MAX_BATCH_SIZE_IN_BYTES = 4_000_000;

    public static final int DEFAULT_BATCH_SIZE_IN_BYTES = 3_670_016;

    private static final String[] VALID_REGIONS = RegionUtils.getRegions()
                                                             .stream()
                                                             .map(Region::getName)
                                                             .toArray(String[]::new);

    private static final Validator REGION_VALIDATOR = ValidString.in(VALID_REGIONS);

    private static final Validator BATCH_SIZE_VALIDATOR = Range.between(0, MAX_BATCH_SIZE);

    private static final Validator BATCH_SIZE_IN_BYTES_VALIDATOR = Range.between(0, MAX_BATCH_SIZE_IN_BYTES);

    private static final Validator MAPPING_FILE_VALIDATOR = (name, value) -> {
        String fileNameOrLocation = (String) value;
        MappingConfigParser.parse(fileNameOrLocation)
                .orElseThrow(() -> new ConfigException(name, value, "Parser could not correctly parse the mapping file"));
    };

    private static final Recommender REGION_RECOMMENDER = regionRecommender();

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addBaseConfig(configDef);
        return configDef;
    }

    private static void addBaseConfig(ConfigDef configDef) {
        int offset = 0;
        final String group = "AWS Configuration";
        configDef.define(
                REGION_CONFIG,
                Type.STRING,
                "us-east-1",
                REGION_VALIDATOR,
                Importance.HIGH,
                "Specify the region of your Kinesis Firehose",
                group,
                ++offset,
                Width.SHORT,
                "AWS Region",
                REGION_RECOMMENDER)
            .define(
                MAPPING_FILE_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                MAPPING_FILE_VALIDATOR,
                Importance.HIGH,
                "Location of the YAML Mapping file that defines the mapping from topics to destinations",
                group,
                ++offset,
                Width.MEDIUM,
                "Mapping Configuration Location")
            .define(
                BATCH_CONFIG,
                Type.BOOLEAN,
                false,
                Importance.HIGH,
                "Should the connector batch messages before sending to Kinesis Firehose?",
                group,
                ++offset,
                Width.SHORT,
                "Batch Send")
            .define(
                BATCH_SIZE_CONFIG,
                Type.INT,
                DEFAULT_BATCH_SIZE,
                    BATCH_SIZE_VALIDATOR,
                Importance.HIGH,
                "Number of messages to be batched together. Firehose accepts at max 500 messages in one batch.",
                group,
                ++offset,
                Width.SHORT,
                "Maximum Number of Messages to Batch")
            .define(
                BATCH_SIZE_IN_BYTES_CONFIG,
                Type.INT,
                DEFAULT_BATCH_SIZE_IN_BYTES,
                    BATCH_SIZE_IN_BYTES_VALIDATOR,
                Importance.HIGH,
                "Message size in bytes when batched together. Firehose accepts at max 4MB in one batch.",
                group,
                ++offset,
                Width.MEDIUM,
                "Maximum Number of Bytes to Batch");
    }

    private static Recommender regionRecommender() {
        return new Recommender() {
            @Override
            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                return Arrays.asList(VALID_REGIONS);
            }

            @Override
            public boolean visible(String name, Map<String, Object> parsedConfig) {
                return true;
            }
        };
    }

    public static ConfigDef CONFIG = baseConfigDef();

    public FirehoseSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
