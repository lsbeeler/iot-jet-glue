package org.hazelcast.iot_jet_glue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

public enum SinkType
{
    LOGGER,
    KAFKA;

    private static final Logger ERROR_LOG = Logger.getLogger(
            SinkType.class.getName( ));

    public static Optional<SinkType> fromProperties(String propsPath,
            String propsKey)
    {
        propsKey = propsKey + ".sink";

        Optional<SinkType> result = Optional.empty( );

        try {
            Properties props = new Properties( );
            props.load(new FileReader(propsPath));
            String propsValue = (String) props.get(propsKey);
            if (propsValue != null)
                result = Optional.of(SinkType.valueOf(propsValue));
        } catch (IOException e) {
            ERROR_LOG.severe("unable to open properties file: " + new File(
                    propsPath).getAbsolutePath( ) + ": " + e.getMessage( ));
        }

        return result;
    }
}
