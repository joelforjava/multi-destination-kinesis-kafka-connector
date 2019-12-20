FROM wurstmeister/kafka:2.12-2.3.0

# TODO - update config files!
# we replace the default connect-standalone.properties so we can properly resolve to our local kafka docker development
COPY config/cluster_1/worker.properties /opt/kafka/config/

COPY config/cluster_1/kinesis-firehose-kafka-connector.properties /opt/kafka/config/

COPY config/cluster_1/streamMapping.yaml /opt/kafka/config/

# we replace the start command creating a connector instead.
COPY start-kafka.sh /usr/bin/

# permissions
RUN chmod a+x /usr/bin/start-kafka.sh
