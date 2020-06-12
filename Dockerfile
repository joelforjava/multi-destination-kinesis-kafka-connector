FROM wurstmeister/kafka:2.12-2.3.0

ARG CLUSTER_NAME=cluster_1

# TODO - how do we handle this in a production environment?
ENV AWS_ACCESS_KEY_ID=fffffffff
ENV AWS_SECRET_ACCESS_KEY=ffffffff

# TODO - update config files!
# we replace the default connect-standalone.properties so we can properly resolve to our local kafka docker development
COPY config/${CLUSTER_NAME}/worker.properties /opt/kafka/config/

COPY config/${CLUSTER_NAME}/kinesis-firehose-kafka-connector.* /opt/kafka/config/

COPY config/${CLUSTER_NAME}/streamMapping.yaml /opt/kafka/config/

COPY target/amazon-kinesis-kafka-connector-*.jar /opt/kafka/plugins/

# we replace the start command creating a connector instead.
COPY start-kafka.sh /usr/bin/

# permissions
RUN chmod a+x /usr/bin/start-kafka.sh
