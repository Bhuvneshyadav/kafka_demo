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
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FileMonitoringService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String KAFKA_TOPIC = "swift-xml-messages";
    private static final String DIRECTORY_PATH = "src/main/resources/messages";
    // Set to track processed files
    private final Set<Path> processedFiles = new HashSet<>();

    public void startWatching() throws IOException, InterruptedException {
        Path path = Paths.get(DIRECTORY_PATH);
        WatchService watchService = FileSystems.getDefault().newWatchService();

        // Register the directory with the WatchService to monitor for specific events
        path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

        System.out.println("Watching directory: " + path.toAbsolutePath());

        // Continuously watch the directory
        while (true) {
            WatchKey key = watchService.take(); // Blocking call, waits for an event to occur
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                // Retrieve the file path from the event
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path fileName = ev.context();
                Path fullPath = path.resolve(fileName);

                // Handle file creation or modification
                if ((kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY)
                        && !processedFiles.contains(fullPath)) {
                    System.out.println("Detected change in file: " + fullPath);
                    processFile(fullPath);
                }
            }

            // Reset the key to continue watching
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
            
            
        }
    }

    private void processFile(Path filePath) {
        try {
            // Read the file content and send it to Kafka
            String content = new String(Files.readAllBytes(filePath));
            kafkaTemplate.send(KAFKA_TOPIC, content);
            System.out.println("Sent file content to Kafka: " + content);

            // After successfully processing the file, delete it
            deleteFile(filePath);

            // Add the file to the set of processed files
            processedFiles.removeAll(processedFiles);
            processedFiles.add(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteFile(Path filePath) {
        try {
            // Attempt to delete the file after processing it
            Files.delete(filePath);
            System.out.println("File deleted successfully: " + filePath);
        } catch (IOException e) {
            System.err.println("Failed to delete file: " + filePath);
            e.printStackTrace();
        }
    }
}

