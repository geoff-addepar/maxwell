package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import org.junit.Test;

import java.util.Properties;

public class MaxwellKafkaProducerWorkerTest {

	@Test
	public void constructNewWorkerWithNullTopic() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "localhost:9092");
		String kafkaTopic = null;
		//shouldn't throw NPE
		new MaxwellKafkaProducerWorker(kafkaProperties, kafkaTopic, null, new MaxwellConfig(), new MaxwellMetrics());
	}
}
