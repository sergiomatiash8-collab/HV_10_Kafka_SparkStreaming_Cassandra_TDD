Wikipedia Real-Time Data Pipeline
🟦 Overview
This project is a resilient, real-time data processing pipeline designed to ingest, transform, and store Wikipedia edit events. The system leverages Python 3.11.9, Kafka as a message broker, Spark Streaming for data transformation, and Apache Cassandra as the final distributed storage. The entire stack is containerized using Docker and Docker Compose for seamless deployment and scalability.

🟩 Test-Driven Development (TDD) Core
The defining characteristic of this project is its strict adherence to Test-Driven Development (TDD).

Methodology: No processing logic was implemented without a corresponding test case.

Verification: We utilized pytest to validate every stage of the pipeline, from event ingestion to final database persistence.

Proven Reliability: Key tests, such as test_wikipedia_event_reaches_kafka, verified the integrity of the data flow early in the development cycle, ensuring that the system remained stable as complexity grew.

🟨 System Architecture
The pipeline is built with a modular, layered approach:

Data Generation: A Python-based generator that streams live Wikipedia events into the input Kafka topic.

Message Broker: A Kafka cluster (managed by Zookeeper) providing high-throughput, fault-tolerant message buffering.

Stream Processing:

Spark Processor: Consumes raw JSON events, performs schema validation and cleansing, and produces enriched data to the processed topic.

Spark-Cassandra Connector: A dedicated streaming sink that persists processed data into the database.

Storage: Apache Cassandra utilizing a pre-defined schema (wiki_namespace.edits) for optimized write performance.

🟧 Automated Orchestration
To simplify infrastructure management, a custom orchestration script was developed to automate the following tasks:

Environment Cleanup: Pruning stale containers and volumes to ensure a clean state.

Image Building: Automating the assembly of Docker images for the Spark processor and data generator.

Schema Initialization: Automatically applying Cassandra keyspace and table definitions via a specialized cassandra-init service.

🟥 Monitoring and Observability
The project integrates a comprehensive monitoring stack to ensure operational transparency:

Prometheus: Scrapes real-time metrics from the Kafka brokers via kafka-exporter.

Grafana: Provides a visual dashboard for critical KPIs:

Topic Throughput: Visualizing the flow of the input and processed topics.

Consumer Lag: Monitoring the delta between the producer and the Spark consumer to identify potential bottlenecks.

🟪 Technical Validation
The pipeline's success was verified through direct database inspection, confirming that the Spark-to-Cassandra bridge is fully operational:

PowerShell
docker exec -it cassandra cqlsh -e "SELECT count(*) FROM wiki_namespace.edits;"
Current Status: The system demonstrates successful data persistence with a steadily increasing record count, confirming the end-to-end reliability of the TDD-validated architecture.