##  San-Francisco-Crime-Statistics---Kafka-and-Spark-Streaming
    SF Crime Statistices with Spark Streaming
    
# Description

In this project, a real-word dataset, extracted from Kaggle, on San Franscisco crime incidents is used to provide statistical analyses using Apache Spark Structured Streaming. A kafka server is created that produces and ingests data through Spark Structured Streaming.

# Requirements

The environment required to set up the projects should be runned in the following manner
•	Spark 2.4.3
•	Scala 2.11.x
•	Java 1.8.x
•	Kafka build with Scala 2.11.x
•	Python 3.6.x or 3.7.x

# Environment Setup

For Macs or Linux:

•	Download Spark from https://spark.apache.org/downloads.html. Choose "Prebuilt for Apache Hadoop 2.7 and later."

•	Unpack Spark in one of your folders (I usually put all my dev requirements in /home/users/user/dev).

•	Download binary for Kafka from this location https://kafka.apache.org/downloads, with Scala 2.11, version 2.3.0. Unzip in your local directory where you unzipped your Spark binary as well. Exploring the Kafka folder, you’ll see the scripts to execute in bin folders, and config files under config folder. You’ll need to modify zookeeper.properties and server.properties. *Download Scala from the official site, or for Mac users, you can also use brew install scala, but make sure you download version 2.11.x.

•	Run below to verify correct versions: java -version scala -version

•	Make sure your ~/.bash_profile looks like below (might be different depending on your directory):

        export SPARK_HOME=/Users/dev/spark-2.4.3-bin-hadoop2.7

        export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home

        export SCALA_HOME=/usr/local/scala/

        export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SCALA_HOME/bin:$PATH
        
For Windows:

Please follow the directions found in this helpful StackOverflow post: https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows

# Running The Project

This project requires starting a zookepeer and kafka servers as well as a kafka bootstrap server.

Step 1

Start Zookeeper and Kafka servers with the following commands below. The config directory contains the configurations for both Zookeeper and Kafka Servers respectively

          bin/zookeeper-server-start.sh config/zookeeper.properties

          bin/kafka-server-start.sh config/server.properties
          
Step 2 

The kafka server is runned to produce data from the police-department-calls-for-service.json into a kafka topic named - "crime_stats"
        
          kafka_server.py
        
Step 3

To confirme that the Kafka Server is producing accurately, a kafka consumer server is created to consume data from the topic - "crime_stats"
          
          consumer_server.py
          
Output of Consumer Server

![]()
          
Step 4

Run the spark job using:

          spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py

Output of Aggregated Crimes

![]()


