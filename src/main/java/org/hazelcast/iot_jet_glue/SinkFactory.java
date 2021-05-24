package org.hazelcast.iot_jet_glue;

import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class SinkFactory
{
    private static final Logger LOGGER = Logger.getLogger(
            "org.hazelcast.iot_jet_glue.SinkFactory");

    private static final String BROKER_PROPERTIES_FILE_PATH =
            "resources/kafka-broker.properties";

    private static String entityNameToStreamName(EntityType entityType)
    {
        return entityType.toString( ).toLowerCase( ) + "s";
    }

    public static Sink<Map.Entry<Long, String>> create(EntityType entityType,
            SinkType sinkType)
    {
        // short-circuit return quickly if the caller only wants a logger sink,
        // otherwise, perform the full Kafka sink creation process
        if (sinkType == SinkType.LOGGER)
            return Sinks.logger( );

        Properties props = KafkaUtils.loadPropertiesForStream(entityType);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        String topicName = entityNameToStreamName(entityType);

        LOGGER.info("created sink for Kafka topic " + topicName);

        return KafkaSinks.<Long, String>kafka(props, topicName);
    }

    public static Sink<Map.Entry<Long, String>> create(EntityType entityType,
            String propsFilePath)
    {
        // Determine whether we write to a console logger or to a Kafka
        // stream from the properties file that was passed in.
        String propsKey = entityNameToStreamName(entityType);

        // If no type was specified in the properties file, default to a
        // Logger sink.
        SinkType sinkType = SinkType.fromProperties(
                "resources/pipeline-config.properties", propsKey)
                                    .orElse(SinkType.LOGGER);

        // Create the appropriate sink.
        return create(entityType, sinkType);
    }
}
