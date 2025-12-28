### Components/Versions

    Java SDK: 11 (LTS)
    Apache Flink: 1.20.0
    Apache Kafka: 3.7.2 (KRaft mode, basically doesn't need Zookeeperdo)
    Docker Engine: Latest (run on Windows 11)
    Maven dependancies:

		org.apache.flink		flink-streaming-java	1.20.0		Core Streaming API
		org.apache.flink		flink-clients		1.20.0		Local execution in IntelliJ
		org.apache.flink		flink-connector-kafka	3.3.0-1.20	Kafka Source/Sink connectivity
		org.apache.flink		flink-connector-base	1.20.0		Required base for Kafka connectors
		org.apache.logging.log4j	log4j-slf4j-impl	2.17.1		Fixed logging bridge (No MDC crash)
		org.apache.logging.log4j	log4j-api		2.17.1		Modern Log4j2 API
		org.apache.logging.log4j	log4j-core		2.17.1		Modern Log4j2 Implementation
		com.fasterxml.jackson.core	jackson-databind	2.17.2		JSON parsing for >10M

### Setup and run

-  Start containers if using Docker

- Open a terminal and type the following to delete, then create the topics needed:	

```  
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic input-tuples;
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic queries;
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic output-skyline;

    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic input-tuples --bootstrap-server localhost:9092;
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic queries --bootstrap-server localhost:9092;
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic output-skyline --bootstrap-server localhost:9092;
```
-  Run FlinkSkyline.java (this will be left running)

-  Run Generator.java to fill the topic with data.

-  In a new termnial, run the below command to see what's happening in the output-skyline 

       docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-skyline --from-beginning

-  In a new terminal, run the below command to trigger a query computation, type numbers like 1, 2, 3 etc to trigger the query.

       docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic queries
