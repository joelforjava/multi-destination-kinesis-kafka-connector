package com.amazon.kinesis.kafka;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.handlers.RequestHandler;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.internal.auth.SignerProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockFirehoseClient extends AmazonKinesisFirehoseClient {

    private final List<Record> records = new ArrayList<>();

    private final List<String> deliveryStreamNames = new ArrayList<>();

    public List<Record> getRecords() {
        return Collections.unmodifiableList(records);
    }

    public List<String> getDeliveryStreamNames() {
        return Collections.unmodifiableList(deliveryStreamNames);
    }

    public MockFirehoseClient() {
    }

    public MockFirehoseClient(ClientConfiguration clientConfiguration) {
    }

    public MockFirehoseClient(AWSCredentials awsCredentials) {
    }

    public MockFirehoseClient(AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
    }

    public MockFirehoseClient(AWSCredentialsProvider awsCredentialsProvider) {
    }

    public MockFirehoseClient(AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
    }

    public MockFirehoseClient(AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricCollector) {
    }

    @Override
    public CreateDeliveryStreamResult createDeliveryStream(CreateDeliveryStreamRequest createDeliveryStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteDeliveryStreamResult deleteDeliveryStream(DeleteDeliveryStreamRequest deleteDeliveryStreamRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeDeliveryStreamResult describeDeliveryStream(DescribeDeliveryStreamRequest describeDeliveryStreamRequest) {
        DescribeDeliveryStreamResult deliveryStreamResult = new DescribeDeliveryStreamResult();
        DeliveryStreamDescription streamDescription = new DeliveryStreamDescription();
        streamDescription.setDeliveryStreamStatus("ACTIVE");
        deliveryStreamResult.setDeliveryStreamDescription(streamDescription);
        return deliveryStreamResult;
    }

    @Override
    public ListDeliveryStreamsResult listDeliveryStreams(ListDeliveryStreamsRequest listDeliveryStreamsRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        deliveryStreamNames.add(putRecordRequest.getDeliveryStreamName());
        records.add(putRecordRequest.getRecord());
        return new PutRecordResult();
    }

    @Override
    public PutRecordBatchResult putRecordBatch(PutRecordBatchRequest putRecordBatchRequest) {
        deliveryStreamNames.add(putRecordBatchRequest.getDeliveryStreamName());
        records.addAll(putRecordBatchRequest.getRecords());
        return new PutRecordBatchResult();
    }

    @Override
    public UpdateDestinationResult updateDestination(UpdateDestinationRequest updateDestinationRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException();
    }

//    @Override
//    protected Signer getSigner() {
//        return super.getSigner();
//    }
//
//    @Override
//    protected SignerProvider getSignerProvider() {
//        return super.getSignerProvider();
//    }
//
//    @Override
//    public void setEndpoint(String endpoint) {
//        super.setEndpoint(endpoint);
//    }
//
//    @Override
//    public void setEndpoint(String endpoint, String serviceName, String regionId) {
//        super.setEndpoint(endpoint, serviceName, regionId);
//    }
//
//    @Override
//    public Signer getSignerByURI(URI uri) {
//        return super.getSignerByURI(uri);
//    }
//
//    @Override
//    public void setRegion(Region region) {
//        super.setRegion(region);
//    }
//
//    @Override
//    public void shutdown() {
//        super.shutdown();
//    }
//
//    @Override
//    public void addRequestHandler(RequestHandler requestHandler) {
//        super.addRequestHandler(requestHandler);
//    }
//
//    @Override
//    public void addRequestHandler(RequestHandler2 requestHandler2) {
//        super.addRequestHandler(requestHandler2);
//    }
//
//    @Override
//    public void removeRequestHandler(RequestHandler requestHandler) {
//        super.removeRequestHandler(requestHandler);
//    }
//
//    @Override
//    public void removeRequestHandler(RequestHandler2 requestHandler2) {
//        super.removeRequestHandler(requestHandler2);
//    }
//
//    @Override
//    protected ExecutionContext createExecutionContext(AmazonWebServiceRequest req) {
//        return super.createExecutionContext(req);
//    }
//
//    @Override
//    protected ExecutionContext createExecutionContext(AmazonWebServiceRequest req, SignerProvider signerProvider) {
//        return super.createExecutionContext(req, signerProvider);
//    }
//
//    @Override
//    protected SignerProvider createSignerProvider(Signer signer) {
//        return super.createSignerProvider(signer);
//    }
//
//    @Override
//    public void setTimeOffset(int timeOffset) {
//        super.setTimeOffset(timeOffset);
//    }
//
//    @Override
//    public AmazonWebServiceClient withTimeOffset(int timeOffset) {
//        return super.withTimeOffset(timeOffset);
//    }
//
//    @Override
//    public int getTimeOffset() {
//        return super.getTimeOffset();
//    }
//
//    @Override
//    public RequestMetricCollector getRequestMetricsCollector() {
//        return super.getRequestMetricsCollector();
//    }
//
//    @Override
//    protected RequestMetricCollector requestMetricCollector() {
//        return super.requestMetricCollector();
//    }
//
//    @Override
//    protected String getServiceAbbreviation() {
//        return super.getServiceAbbreviation();
//    }
//
//    @Override
//    public String getServiceName() {
//        return super.getServiceName();
//    }
//
//    @Override
//    public String getEndpointPrefix() {
//        return super.getEndpointPrefix();
//    }
//
//    @Override
//    protected void setEndpointPrefix(String endpointPrefix) {
//        super.setEndpointPrefix(endpointPrefix);
//    }
//
//    @Override
//    protected String getServiceNameIntern() {
//        return super.getServiceNameIntern();
//    }
//
//    @Override
//    public <T extends AmazonWebServiceClient> T withRegion(Region region) {
//        return super.withRegion(region);
//    }
//
//    @Override
//    public <T extends AmazonWebServiceClient> T withRegion(Regions region) {
//        return super.withRegion(region);
//    }
//
//    @Override
//    public <T extends AmazonWebServiceClient> T withEndpoint(String endpoint) {
//        return super.withEndpoint(endpoint);
//    }
//
//    @Override
//    protected boolean useStrictHostNameVerification() {
//        return super.useStrictHostNameVerification();
//    }
//
//    @Override
//    protected boolean calculateCRC32FromCompressedData() {
//        return super.calculateCRC32FromCompressedData();
//    }
//
//    @Override
//    public String getSignerOverride() {
//        return super.getSignerOverride();
//    }
//
//    @Override
//    public int hashCode() {
//        return super.hashCode();
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        return super.equals(obj);
//    }
//
//    @Override
//    protected Object clone() throws CloneNotSupportedException {
//        return super.clone();
//    }
//
//    @Override
//    public String toString() {
//        return super.toString();
//    }
//
//    @Override
//    protected void finalize() throws Throwable {
//        super.finalize();
//    }
}
