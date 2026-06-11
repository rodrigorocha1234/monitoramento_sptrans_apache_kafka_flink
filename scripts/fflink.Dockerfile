FROM flink:2.2.0-scala_2.12

USER root

RUN mkdir -p /opt/flink/lib

# JDBC Core
RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar

# JDBC PostgreSQL
RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.0.0-2.0/flink-connector-jdbc-postgres-4.0.0-2.0.jar

# Driver PostgreSQL
RUN wget -P /opt/flink/lib \
https://jdbc.postgresql.org/download/postgresql-42.7.7.jar


# Kafka
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.1-2.0/flink-connector-kafka-4.0.1-2.0.jar

# Flink Avro
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-avro/2.2.0/flink-avro-2.2.0.jar

# Apache Avro
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/avro-1.12.0.jar

# Confluent Registry
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/2.2.0/flink-avro-confluent-registry-2.2.0.jar

# Jackson
RUN wget -P /opt/flink/lib \
 https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.18.4/jackson-core-2.18.4.jar

RUN wget -P /opt/flink/lib \
 https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.18.4/jackson-annotations-2.18.4.jar

RUN wget -P /opt/flink/lib \
 https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.18.4/jackson-databind-2.18.4.jar

# Kafka clients (FALTAVA ISSO)
RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar


RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-json/2.2.0/flink-json-2.2.0.jar


RUN wget -P /opt/flink/lib \
https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.0/kafka-schema-registry-client-7.6.0.jar

RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/com/google/guava/guava/32.1.3-jre/guava-32.1.3-jre.jar

USER flink