package com.kafka;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerExample {
	
    public void kafkaProduce() throws IOException {
        // Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Directory to watch
        String watchDir = "src/main/resources";
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Paths.get(watchDir).register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        System.out.println("Watching directory: " + watchDir);

        while (true) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    Path filePath = ((Path) event.context()).toAbsolutePath();
                    System.out.println("New file detected: " + filePath);

                    // Read file contents
                    try {
                        String content = Files.readString(Paths.get(watchDir, filePath.toString()));

                        // Send content as a Kafka message
                        String topic = "swift-xml-messages";
                        producer.send(new ProducerRecord<>(topic, content));
                        System.out.println("File content sent to Kafka!");
                    } catch (IOException e) {
                        System.err.println("Failed to read file: " + e.getMessage());
                    }
                }
            }

            // Reset key and remove from the set if invalid
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }

        producer.close();
    }
}
