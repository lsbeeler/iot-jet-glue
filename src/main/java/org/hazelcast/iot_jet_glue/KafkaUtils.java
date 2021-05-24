package org.hazelcast.iot_jet_glue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaUtils
{
    private static final Logger LOGGER = Logger.getLogger(
            "org.hazelcast.iot_jet_glue.KafkaUtils");

    private static final String BROKER_PROPERTIES_FILE_PATH =
            "resources/kafka-broker.properties";

    public static String entityNameToStreamName(EntityType entityType)
    {
        return entityType.toString( ).toLowerCase( ) + "s";
    }

    public static Properties loadPropertiesForStream(EntityType streamEntity)
    {
        Properties props = new Properties( );

        try {
            props.load(new FileReader(BROKER_PROPERTIES_FILE_PATH));
            props.put("acks", "all");

            String streamName = (String) props.get("client.id");
            if (streamName == null)
                streamName = "hazelcast_jet_telematics";
            streamName = streamName + "-" + entityNameToStreamName(streamEntity);
            LOGGER.info("creating Kafka client id " + streamName);
            props.put("client.id", streamName);
        } catch (IOException e) {
            System.err.println("unable to load Kafka properties from file " +
                    new File(BROKER_PROPERTIES_FILE_PATH).getAbsolutePath( ) +
                    ": " + e.getMessage( ));
            e.printStackTrace( );
            System.exit(127);
        }

        return props;
    }

    public static Producer<String, String> createArbitraryProducer(
            EntityType streamType, Properties kafkaProps)
    {
        Properties producerProps = loadPropertiesForStream(streamType);

        producerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(producerProps);
    }
}
