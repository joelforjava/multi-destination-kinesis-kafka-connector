package com.amazon.kinesis.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.kinesis.kafka.config.FirehoseSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirehoseSinkConnector extends SinkConnector {

    private static Logger log = LoggerFactory.getLogger(FirehoseSinkConnector.class);

	private Map<String, String> configProperties;

	@Override
	public void start(Map<String, String> props) {
	    log.debug("Config map: " + props);
		try {
			configProperties = props;
			new FirehoseSinkConnectorConfig(props);
		} catch (ConfigException ce) {
			throw new ConfigException("Unable to start FirehoseSinkConnector due to configuration error", ce);
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public Class<? extends Task> taskClass() {
		return FirehoseSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> configs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			configs.add(taskProps);
		}
		return configs;
	}

	@Override
	public String version() {
		// Currently using Kafka version, in future release use Kinesis-Kafka version
		return AppInfoParser.getVersion();
	}

	@Override
	public ConfigDef config() {
		return FirehoseSinkConnectorConfig.CONFIG;
	}

}
