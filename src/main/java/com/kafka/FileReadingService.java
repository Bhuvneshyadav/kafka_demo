package com.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

@Service
public class FileReadingService {

    
    public StringBuffer readFile() throws IOException {
        StringBuffer lines = new StringBuffer();

        // Open the input stream from the resource
        ClassPathResource resource = new ClassPathResource("xmlMessage");
        
        System.out.println("Classpath: " + resource);
        try (InputStream inputStream = resource.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lines.append(line).append(System.lineSeparator());
            }
        }

        return lines;
    }
}

