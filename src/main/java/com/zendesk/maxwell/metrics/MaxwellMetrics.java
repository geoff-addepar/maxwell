package com.zendesk.maxwell.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.util.TaskManager;
import org.apache.commons.lang.StringUtils;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.coursera.metrics.datadog.transport.Transport;
import org.coursera.metrics.datadog.transport.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.coursera.metrics.datadog.DatadogReporter.Expansion.*;

public class MaxwellMetrics {
	private static final String reportingTypeSlf4j = "slf4j";
	private static final String reportingTypeJmx = "jmx";
	private static final String reportingTypeHttp = "http";
	private static final String reportingTypeDataDog = "datadog";

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellMetrics.class);

	private final MetricRegistry metricRegistry;
	private final HealthCheckRegistry healthCheckRegistry;

	private final String metricsPrefix;

	public MaxwellMetrics() {
		this(new MetricRegistry(), new HealthCheckRegistry(), "MaxwellMetrics");
	}

	public MaxwellMetrics(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, String metricsPrefix) {
		this.metricRegistry = metricRegistry;
		this.healthCheckRegistry = healthCheckRegistry;
		this.metricsPrefix = metricsPrefix;
	}

	public static MaxwellMetrics fromConfig(MaxwellConfig config, TaskManager taskManager) {
		if (config.maxwellMetrics != null) {
			return config.maxwellMetrics;
		}

		String metricsPrefix = config.metricsPrefix;
		MetricRegistry metricRegistry = new MetricRegistry();
		HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

		if (config.metricsReportingType == null) {
			LOGGER.warn("Metrics will not be exposed: metricsReportingType not configured.");
			return new MaxwellMetrics(metricRegistry, healthCheckRegistry, metricsPrefix);
		}

		if (config.metricsReportingType.contains(reportingTypeSlf4j)) {
			final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
					.outputTo(LOGGER)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();

			reporter.start(config.metricsSlf4jInterval, TimeUnit.SECONDS);
			LOGGER.info("Slf4j metrics reporter enabled");
		}

		if (config.metricsReportingType.contains(reportingTypeJmx)) {
			final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
					.convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS)
					.build();
			jmxReporter.start();
			LOGGER.info("JMX metrics reporter enabled");

			if (System.getProperty("com.sun.management.jmxremote") == null) {
				LOGGER.warn("JMX remote is disabled");
			} else {
				String portString = System.getProperty("com.sun.management.jmxremote.port");
				if (portString != null) {
					LOGGER.info("JMX running on port " + Integer.parseInt(portString));
				}
			}
		}

		if (config.metricsReportingType.contains(reportingTypeHttp)) {
			LOGGER.info("Metrics http server starting");
			new MaxwellHTTPServer(config.metricsHTTPPort, metricRegistry, healthCheckRegistry, taskManager);
			LOGGER.info("Metrics http server started on port " + config.metricsHTTPPort);
		}

		if (config.metricsReportingType.contains(reportingTypeDataDog)) {
			Transport transport;
			if (config.metricsDatadogType.contains("http")) {
				LOGGER.info("Enabling HTTP Datadog reporting");
				transport = new HttpTransport.Builder()
						.withApiKey(config.metricsDatadogAPIKey)
						.build();
			} else {
				LOGGER.info("Enabling UDP Datadog reporting with host " + config.metricsDatadogHost
						+ ", port " + config.metricsDatadogPort);
				transport = new UdpTransport.Builder()
						.withStatsdHost(config.metricsDatadogHost)
						.withPort(config.metricsDatadogPort)
						.build();
			}

			final DatadogReporter reporter = DatadogReporter.forRegistry(metricRegistry)
					.withTransport(transport)
					.withExpansions(EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99))
					.withTags(getDatadogTags(config.metricsDatadogTags))
					.build();

			reporter.start(config.metricsDatadogInterval, TimeUnit.SECONDS);
			LOGGER.info("Datadog reporting enabled");
		}

		return new MaxwellMetrics(metricRegistry, healthCheckRegistry, metricsPrefix);
	}

	private static ArrayList<String> getDatadogTags(String datadogTags) {
		ArrayList<String> tags = new ArrayList<>();
		for (String tag : datadogTags.split(",")) {
			if (!StringUtils.isEmpty(tag)) {
				tags.add(tag);
			}
		}

		return tags;
	}

	public Timer timer(String... names) {
		return metricRegistry.timer(MetricRegistry.name(metricsPrefix, names));
	}

	public Counter counter(String... names) {
		return metricRegistry.counter(MetricRegistry.name(metricsPrefix, names));
	}

	public Meter meter(String... names) {
		return metricRegistry.meter(MetricRegistry.name(metricsPrefix, names));
	}

	public void gauge(Gauge<?> gauge, String... names) {
		metricRegistry.register(MetricRegistry.name(metricsPrefix, names), gauge);
	}

	public void healthCheck(String name, HealthCheck healthCheck) {
		healthCheckRegistry.register(name, healthCheck);
	}
}
