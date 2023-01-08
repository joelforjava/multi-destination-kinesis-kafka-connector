package com.amazon.kinesis.kafka;

import com.amazonaws.services.kinesisfirehose.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FirehoseRecordProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseRecordProcessor.class);

    private int batchSize = 500;

    private int batchSizeInBytes = 3929834; // setting to random values for now

    /**
     * Process a Map that contains records to be processed.
     *
     * The map is keyed by the destination stream names, with the values being the records to send to
     * that destination.
     *
     * @param recordList - a Map that contains records to be sent to different firehose destination streams.
     */
    public void processRecords(Map<String, List<Record>> recordList) {
        recordList.forEach(this::processRecordsForDestination);
    }

    public void processRecordsForDestination(String destinationStreamName, List<Record> records) {
        List<Record> accumulator = new ArrayList<>(batchSize);  // TODO - can we fine tune the initial size?

        int recordsInBatch = 0;
        int recordsSizeInBytes = 0;

        for (Record record : records) {
            accumulator.add(record);
            recordsInBatch++;
            recordsSizeInBytes += record.getData().capacity();

            if (recordsInBatch == batchSize || recordsSizeInBytes > batchSizeInBytes) {
                putRecordBatch(destinationStreamName, accumulator);
                accumulator.clear();
                recordsInBatch = 0;
                recordsSizeInBytes = 0;
            }
        }

        if (recordsInBatch > 0) {
            putRecordBatch(destinationStreamName, accumulator);
        }

    }

    // NOTE: Placeholder
    public void putRecordBatch(String destinationStreamName, List<Record> records) {

    }
}
