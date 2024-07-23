# Kafka to Snowflake Pipeline with Spark

## Overview

This project sets up a data ingestion pipeline that uses Apache Kafka for streaming data production, Apache Spark for processing, and Snowflake for data storage. The pipeline efficiently manages data flow from Kafka to Snowflake, ensuring real-time data ingestion and processing.

### Components
- **Apache Kafka**: Used for producing and consuming streaming data.
- **Apache Spark**: Handles real-time data processing.
- **Snowflake**: Stores the processed data.

## Architecture

1. **Apache Kafka**
   - **Producer**: Generates and publishes streaming data to Kafka topics.
   - **Consumer**: Consumed by Spark Streaming for processing.

2. **Apache Spark**
   - **Stream Processing**: Reads data from Kafka topics.
   - **foreachBatch**: Writes the processed data to Snowflake.

3. **Snowflake**
   - **Storage**: Receives and stores the processed data from Spark.

### Data Flow
1. **Kafka Producer**:
   - Data is generated and sent to Kafka topics.

2. **Spark Streaming**:
   - Reads streaming data from Kafka topics.
   - Processes the data in real-time using Spark.
   - Writes the processed data to Snowflake using the `foreachBatch` method.

3. **Snowflake**:
   - Stores the data ingested from Spark.
  
## License

This project is licensed under the [MIT License](LICENSE).
