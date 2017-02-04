#!/bin/bash

export SPARK_CLASSPATH=$SPARK_HOME/external/kafka/target/spark-streaming-kafka_2.10-1.6.1.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1.jar:\
$KAFKA_HOME/libs/aopalliance-repackaged-2.4.0-b31.jar:\
$KAFKA_HOME/libs/argparse4j-0.5.0.jar:\
$KAFKA_HOME/libs/connect-api-0.9.0.1.jar:\
$KAFKA_HOME/libs/connect-file-0.9.0.1.jar:\
$KAFKA_HOME/libs/connect-json-0.9.0.1.jar:\
$KAFKA_HOME/libs/connect-runtime-0.9.0.1.jar:\
$KAFKA_HOME/libs/hk2-api-2.4.0-b31.jar:\
$KAFKA_HOME/libs/hk2-locator-2.4.0-b31.jar:\
$KAFKA_HOME/libs/hk2-utils-2.4.0-b31.jar:\
$KAFKA_HOME/libs/jopt-simple-3.2.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1-javadoc.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1-scaladoc.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1-sources.jar:\
$KAFKA_HOME/libs/kafka_2.10-0.9.0.1-test.jar:\
$KAFKA_HOME/libs/kafka-clients-0.9.0.1.jar:\
$KAFKA_HOME/libs/kafka-log4j-appender-0.9.0.1.jar:\
$KAFKA_HOME/libs/kafka-tools-0.9.0.1.jar:\
$KAFKA_HOME/libs/log4j-1.2.17.jar:\
$KAFKA_HOME/libs/lz4-1.2.0.jar:\
$KAFKA_HOME/libs/metrics-core-2.2.0.jar:\
$KAFKA_HOME/libs/osgi-resource-locator-1.0.1.jar:\
$KAFKA_HOME/libs/slf4j-api-1.7.6.jar:\
$KAFKA_HOME/libs/slf4j-log4j12-1.7.6.jar:\
$KAFKA_HOME/libs/snappy-java-1.1.1.7.jar:\
$KAFKA_HOME/libs/validation-api-1.1.0.Final.jar:\
$KAFKA_HOME/libs/zkclient-0.7.jar:\
$KAFKA_HOME/libs/zookeeper-3.4.6.jar:\
/home/billtsay/CS9223/joda-time-2.4/joda-time-2.4.jar:\
/home/billtsay/CS9223/gson-master/gson/target/gson-2.6.3-SNAPSHOT.jar:\
/home/billtsay/CS9223/bin/guava-19.0.jar:\
/home/billtsay/CS9223/jsr166e-master/target/jsr166e-1.1.0.jar:\
/home/billtsay/CS9223/java-driver-3.0/driver-core/target/cassandra-driver-core-3.0.1-SNAPSHOT.jar:\
/home/billtsay/CS9223/spark-cassandra-connector-master/spark-cassandra-connector-1.6.0-M2-s_2.10.jar:\
$SPARK_HOME/examples/target/spark-examples_2.10-1.6.1.jar

$SPARK_HOME/bin/spark-submit --class $1 \
--master local[8] /home/billtsay/CS9223/bin/stockquote.jar \
$2 $3 $4 $5

