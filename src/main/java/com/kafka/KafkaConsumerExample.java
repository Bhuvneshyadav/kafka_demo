package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Collections;
import java.util.Properties;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class KafkaConsumerExample {
	
    public static void main(String[] args) {
        // Kafka consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "swift-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("swift-xml-messages"));

        System.out.println("Listening for messages...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: %s%n", record.value());
                    processXmlMessage(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void processXmlMessage(String xmlMessage) {
        try {
            // Parse XML
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new java.io.ByteArrayInputStream(xmlMessage.getBytes()));

            // Extract elements
            Element root = document.getDocumentElement();
            String sender = root.getElementsByTagName("Sender").item(0).getTextContent();
            String receiver = root.getElementsByTagName("Receiver").item(0).getTextContent();
            String amount = root.getElementsByTagName("Amount").item(0).getTextContent();
            String currency = root.getElementsByTagName("Currency").item(0).getTextContent();

            System.out.printf("Parsed XML -> Sender: %s, Receiver: %s, Amount: %s, Currency: %s%n",
                    sender, receiver, amount, currency);
        } catch (Exception e) {
            System.err.printf("Failed to process XML: %s%n", e.getMessage());
        }
    }
}
