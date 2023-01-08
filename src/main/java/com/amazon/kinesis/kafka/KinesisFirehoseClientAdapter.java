package com.amazon.kinesis.kafka;

import com.amazon.kinesis.kafka.config.FirehoseSinkConnectorConfig;
import com.amazon.kinesis.kafka.enums.KinesisFirehoseServiceErrors;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KinesisFirehoseClientAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisFirehoseClientAdapter.class);

    private static final List<Integer> RETRYABLE_STATUS_CODES = Arrays.asList(429, 500, 502, 503, 504);

    private static final int RETRY_MULTIPLIER = 5;

    private final int throttlingBackoffMs;

    private final AmazonKinesisFirehoseClient firehoseClient;

    /**
     * Given a map of properties, create a new adapter.
     *
     * Create a new adapter and use a default firehose client, which uses the default credentials provider chain.
     *
     * @param props - key/value pairs provided by Kafka Connect that contains expected properties for AWS Region and
     *              Throttling Backoff in Milliseconds. If the backoff is not provided, a default value is used.
     */
    public KinesisFirehoseClientAdapter(Map<String, String> props) {
        this(new AmazonKinesisFirehoseClient(
                new DefaultAWSCredentialsProviderChain()
        ).withRegion(Regions.fromName(props.get(FirehoseSinkConnectorConfig.REGION_CONFIG))),
                props.getOrDefault(FirehoseSinkConnectorConfig.BACKOFF_MILLIS_CONFIG, "" + FirehoseSinkConnectorConfig.DEFAULT_BACKOFF_MILLIS));  // TODO - come back and add the backoff setups
    }

    public KinesisFirehoseClientAdapter(String regionName, String backoffMillis) {
        this.throttlingBackoffMs = Integer.parseInt(backoffMillis);
        // AmazonKinesisFirehoseClientBuilder.standard().withRegion(RegionUtils.getRegion(regionName).toString()).build();
        this.firehoseClient = new AmazonKinesisFirehoseClient(new DefaultAWSCredentialsProviderChain());
        this.firehoseClient.setRegion(RegionUtils.getRegion(regionName));
    }

    public KinesisFirehoseClientAdapter(AmazonKinesisFirehoseClient client, Map<String, String> props) {
        this(client,
                props.getOrDefault(FirehoseSinkConnectorConfig.BACKOFF_MILLIS_CONFIG, "" + FirehoseSinkConnectorConfig.DEFAULT_BACKOFF_MILLIS));
    }

    public KinesisFirehoseClientAdapter(AmazonKinesisFirehoseClient client, String backoffMillis) {
        this(client, Integer.parseInt(backoffMillis));
    }

    public KinesisFirehoseClientAdapter(AmazonKinesisFirehoseClient client, int throttlingBackoffMs) {
        this.throttlingBackoffMs = throttlingBackoffMs;
        this.firehoseClient = client;
    }

    /**
     * Given a streamName, retrieve the description of it from the AWS Kinesis firehose service.
     *
     * @param streamName The name of the firehose on which we wish to retrieve information
     * @return DescribeDeliveryStreamResult, the response from the firehose service
     */
    public DescribeDeliveryStreamResult describeDeliveryStream(String streamName) {
        DescribeDeliveryStreamRequest request = new DescribeDeliveryStreamRequest();
        request.setDeliveryStreamName(streamName);
        return firehoseClient.describeDeliveryStream(request);
    }

    public void sendRecords(Collection<SinkRecord> sinkRecords, String streamName) {
        // NOTE: This should not be used as-is. All of our deployed connectors should be using batch mode instead.
        //    This method exists just for completeness' sake and is not intended for use at this time.
        //    Also note the signature is a Kafka sink record and not the Kinesis Firehose Model record. This should
        //    be updated to match if the method is actually implemented for production use.
        for (SinkRecord record : sinkRecords) {
            PutRecordRequest request = new PutRecordRequest();
            request.setDeliveryStreamName(streamName);
            request.setRecord(DataUtility.createRecord(record));

            try {
                firehoseClient.putRecord(request);
            } catch(ResourceNotFoundException rnfe) {
                LOGGER.error("Firehose not found: {}. {}", streamName, rnfe.getLocalizedMessage());
                throw rnfe;
            } catch(AmazonKinesisFirehoseException akfe) {
                LOGGER.error("Amazon Kinesis Firehose Exception: " + akfe.getLocalizedMessage());
                throw akfe;
            } catch(Exception e) {
                LOGGER.error("Unexpected Connector Exception: " + e.getLocalizedMessage());
                throw e;
            }
            // TODO - I think there's now another exception here. Or, it might just be in the batch processor.
        }
    }

    /**
     * Send a list of Kinesis Firehose records to the Delivery Stream represented by streamName.
     *
     * In the event of a Firehose Service exception, will attempt to retry the request via recursive calls after
     * sleeping for throttlingBackoffMs milliseconds.
     *
     * @param recordList The list of records to send.
     * @param streamName The name of the stream to which to send the records.
     * @return PutRecordBatchResult, the response from the firehose service
     */
    public PutRecordBatchResult sendRecordsAsBatch(List<Record> recordList, String streamName) {
        LOGGER.debug("[TRYING] stream: " + streamName + " record count: " + recordList.size());

        final PutRecordBatchResult result;
        try {
            result = processServiceRequest(recordList, streamName);
        } catch (InvalidArgumentException iae) {
            LOGGER.warn("Invalid Argument Exception: {}. Retrying by splitting the payload", iae.getLocalizedMessage());
            this.sendRecordsAsBatch(recordList.subList(0, 1), streamName);
            return this.sendRecordsAsBatch(recordList.subList(1, recordList.size()), streamName);
        } catch (ResourceNotFoundException rnfe) {
            LOGGER.error("Firehose Not Found: {}. {}", streamName, rnfe.getLocalizedMessage());
            throw rnfe;
        } catch (AmazonServiceException e) {
            LOGGER.error("Amazon Service Exception: " + e.getLocalizedMessage());
            String failureCode = e.getErrorCode();
            int statusCode = e.getStatusCode();
            if (RETRYABLE_STATUS_CODES.contains(statusCode) || KinesisFirehoseServiceErrors.isRetryable(failureCode)) {
                LOGGER.warn(
                        "Service Unavailable for Firehose '{}' with HTTP Status {} and Error code {}",
                        streamName, statusCode, failureCode);
                try {
                    int retryMs = throttlingBackoffMs;
                    if (retryMs < 50000) {
                        retryMs = throttlingBackoffMs * RETRY_MULTIPLIER;
                    }
                    LOGGER.warn("Slowing Down {}ms before re-attempting request", retryMs);
                    Thread.sleep(retryMs);
                } catch (InterruptedException ie) {
                    LOGGER.warn("Sleep interrupted");
                }
                LOGGER.info("[RETRYING] stream: {} record count: {}", streamName, recordList.size());
                return this.sendRecordsAsBatch(recordList, streamName);
            } else {
                throw e;
            }
        } catch (SdkClientException e) {
            LOGGER.error("SDK Client Exception: {}", e.getLocalizedMessage());
            try {
                LOGGER.warn("Slowing Down {}ms before re-attempting request", throttlingBackoffMs);
                Thread.sleep(throttlingBackoffMs);
            } catch (InterruptedException ie) {
                LOGGER.warn("Sleep interrupted");
            }
            LOGGER.info("[RETRYING] stream: {} record count: {}", streamName, recordList.size());
            return this.sendRecordsAsBatch(recordList, streamName);
        } catch (Exception e) {
            LOGGER.error("Unexpected Connector Exception: " + e.getLocalizedMessage());
            throw e;
        }
        return result;
    }

    /**
     * Makes a request to the Firehose service and processes the response.
     *
     * In the event the Firehose service rejects one or more records, the failed records will be retried
     * via an amended call to sendRecordsBatch. This process continues until all failed records are accepted.
     *
     * @param recordList The list of records to send.
     * @param streamName The name of the stream to which to send the records.
     * @return PutRecordBatchResult, the response from the firehose service.
     */
    private PutRecordBatchResult processServiceRequest(List<Record> recordList, String streamName) {
        PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(streamName);
        putRecordBatchRequest.setRecords(recordList);

        PutRecordBatchResult result = firehoseClient.putRecordBatch(putRecordBatchRequest);
        int failedCount = result.getFailedPutCount();

        if (failedCount > 0) {
            LOGGER.warn("Failed sending {} messages. Will retry failed messages for {}.", failedCount, putRecordBatchRequest.getRecords().size());
            List<Record> retries = new ArrayList<>();
            boolean shouldSlowDown = false;
            List<PutRecordBatchResponseEntry> requestResponses = result.getRequestResponses();
            for (int ii = 0;ii < requestResponses.size(); ii++) {
                PutRecordBatchResponseEntry entry = requestResponses.get(ii);
                String recordId = entry.getRecordId();
                if (recordId == null || recordId.trim().isEmpty()) {
                    String errCode = entry.getErrorCode();
                    if (!shouldSlowDown && KinesisFirehoseServiceErrors.UNAVAILABLE.name().equals(errCode)) {
                        shouldSlowDown = true;
                    }
                    retries.add(recordList.get(ii));
                }
            }
            if (retries.size() > 0) {
                if (shouldSlowDown) {
                    try {
                        LOGGER.warn("Slowing down {}ms...", throttlingBackoffMs);
                        Thread.sleep(throttlingBackoffMs);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Sleep interrupted");
                    }
                }
                LOGGER.info("[RETRYING] stream: {} record count: {}", streamName, retries.size());
                this.sendRecordsAsBatch(retries, streamName);
            }
        }
        int successCount = recordList.size() - failedCount;
        if (successCount > 0) {
            LOGGER.info("[SUCCESS] stream: {} record count: {}", streamName, successCount);
        }
        if (failedCount > 0) {
            LOGGER.info("[RETRYING] Complete for {}", streamName);
        }
        return result;
    }
}
