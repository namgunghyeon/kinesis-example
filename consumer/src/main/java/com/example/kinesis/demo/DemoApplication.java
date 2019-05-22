package com.example.kinesis.demo;

import java.util.UUID;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {
	@Value("${aws.accessKeyId}")
	private String accessKeyId;

	@Value("${aws.secretKey}")
	private String secretKey;

	@Value("${transmitter.kinesis.endpoint}")
	private String endpoint;

	@Value("${transmitter.kinesis.regionId}")
	private String regionId;

	@Value("${transmitter.kinesis.streamName}")
	private String streamName;

	@Value("${transmitter.kinesis.applicationName}")
	private String applicationName;

	@Value("${transmitter.kinesis.failuresToTolerate}")
	private int failuresToTolerate;

	@Value("${transmitter.kinesis.idleTimeBetweenReadsInMillis}")
	private long idleTimeBetweenReadsInMillis;

	@Autowired
	private KinesisClientLibConfiguration kinesisClientLibConfiguration;

	@Autowired
	private IRecordProcessorFactory recordProcessorFactory;


	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... args) {
		start();
	}

	private void start()  {
		final Worker worker = new Worker.Builder()
				.recordProcessorFactory(recordProcessorFactory)
				.config(kinesisClientLibConfiguration)
				.build();

		worker.run();
	}

	private String createWorkerId() {
		return "test:" + UUID.randomUUID();
	}

	@Bean
	public KinesisClientLibConfiguration kinesisClientLibConfiguration() {
		java.security.Security.setProperty("networkaddress.cache.ttl", "60");

		System.setProperty("aws.accessKeyId", accessKeyId);
		System.setProperty("aws.secretKey", secretKey);

		String id = createWorkerId();

		return new KinesisClientLibConfiguration(applicationName, streamName,
				new DefaultAWSCredentialsProviderChain(), id)
				.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
				.withKinesisEndpoint(endpoint)
				.withRegionName(regionId)
				.withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis);
	}

}
