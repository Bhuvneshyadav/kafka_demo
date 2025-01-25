# kafka_demo
🚀 Enhancing File Monitoring with Kafka Integration 🌟



💻 Just wrapped up a project where we implemented a file monitoring service in Java, integrated seamlessly with Apache Kafka. Here’s a quick overview of the journey:

🔍 What does this service do?

The service continuously monitors a directory (src/main/resources/messages) for file creation or modification events using the WatchService API.



🌐 How does it work?

1️⃣ Detects changes in files within the directory.

2️⃣ Read the content of the new/modified files.

3️⃣ Sends the file content as a message to a Kafka topic (swift-xml-messages).

4️⃣ Cleans up by deleting the processed files to ensure smooth operation.



🎯 Key Highlights:

Real-Time Event Processing: Handles events as they occur, ensuring no file changes are missed.

Seamless Kafka Integration: Utilizes KafkaTemplate from Spring to send file content directly to a Kafka topic, making it perfect for distributed processing pipelines.

Robustness: Tracks processed files and gracefully handles exceptions like I/O issues.



🛠️ Tech Stack:

Java: Core logic for file monitoring and processing.

Spring Kafka: Simplifies interaction with Kafka.



WatchService API: Efficient file system event monitoring.

💡 Real-World Applications:

📤 Streaming large datasets into Kafka for further processing.

🔄 Automating workflows where file changes act as triggers.

📈 Enabling near-real-time processing pipelines for system integrations.

