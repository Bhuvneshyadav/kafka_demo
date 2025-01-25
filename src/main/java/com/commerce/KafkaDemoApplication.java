package com.commerce;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.kafka.FileMonitoringService;
import com.kafka.FileReadingService;
import com.kafka.KafkaProducerExample;

@SpringBootApplication
@ComponentScan(basePackages = {"com.commerce", "com.kafka"})
public class KafkaDemoApplication implements CommandLineRunner {
	
private final FileReadingService fileReadingService;

private KafkaProducerExample  kafkaproducer;
@Autowired
private FileMonitoringService fileMonitoringService;

	    // Constructor injection
	    public KafkaDemoApplication(FileReadingService fileReadingService, KafkaProducerExample kafkaproducer) {
	        this.fileReadingService = fileReadingService;
	        this.kafkaproducer= kafkaproducer;
	    }

	    public static void main(String[] args) {
	        // Start Spring Boot application, initializing the application context
	        SpringApplication.run(KafkaDemoApplication.class, args);
	    }

	    @Override
	    public void run(String... args) throws Exception {
	        // Call the readFile method and print the content
	    	//kafkaproducer.kafkaProduce();
	    	 fileMonitoringService.startWatching();
	    }
	   
	}

