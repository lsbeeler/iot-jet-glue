package org.hazelcast.iot_jet_glue;

import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Sink;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaSinkFactory
{
    private static final String BROKER_PROPERTIES_FILE_PATH =
            "resources/kafka-broker.properties";

    public static Sink<Map.Entry<Long, String>> create(String topicName)
    {
        Properties props = new Properties( );

        try {
            props.load(new FileReader(BROKER_PROPERTIES_FILE_PATH));
            props.put("acks", "all");
            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.LongSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        } catch (IOException e) {
            System.err.println("unable to load Kafka properties from file " +
                    new File(BROKER_PROPERTIES_FILE_PATH).getAbsolutePath( ) +
                    ": " + e.getMessage( ));
            e.printStackTrace( );
            System.exit(127);
        }

        return KafkaSinks.kafka(props, topicName);
    }
}
