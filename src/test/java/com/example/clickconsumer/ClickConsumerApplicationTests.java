package com.example.clickconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableKafka
@EmbeddedKafka(
		partitions = 1,
		topics = { "pageClickTopic" },
		brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" }
)
@SpringBootTest
class ClickConsumerApplicationTests {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	private static final String PAGE_CLICK_TOPIC = "pageClickTopic";

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;

	@Test
	void contextLoads() {


		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		DefaultKafkaProducerFactory<String, PageClickEvent> pf = new DefaultKafkaProducerFactory<>(producerProps);
		Producer<String, PageClickEvent> producer = pf.createProducer();
		producer.send(new ProducerRecord<>(PAGE_CLICK_TOPIC,  new PageClickEvent("Tetsuya", "HomeScreen")));

		System.setOut(new PrintStream(outContent));

		// Wait for event to be consumed
		try {
			Thread.sleep(3000L);
		} catch (Exception ex) {
		}

		assertTrue(outContent.toString().contains("Consumed : PageClickEventForConsumer(page=HomeScreen)"));

		System.setOut(originalOut);

	}

}
