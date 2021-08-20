package com.example.clickconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@SpringBootApplication
public class ClickConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ClickConsumerApplication.class, args);
	}

	@KafkaListener(id = "consumerId", topics = "pageClickTopic")
	public void listen(PageClickEventForConsumer pageClickEventForConsumer) {
		System.out.println("Consumed : " + pageClickEventForConsumer.toString());
	}

	@Bean
	public StringJsonMessageConverter jsonConverter() {
		return new StringJsonMessageConverter();
	}

}
