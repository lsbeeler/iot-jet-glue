package org.hazelcast.iot_jet_glue;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;

import java.io.File;
import java.io.IOException;

public class RemoteIMDGConfigFactory
{
    private static final String REMOTE_CONFIG_PATH ="resources/remote-imdg.xml";
    private static final File REMOTE_CONFIG_FILE = new File(REMOTE_CONFIG_PATH);

    public static ClientConfig createConfig( )
    {
        ClientConfig remoteInstanceConfig = null;
        try {
            remoteInstanceConfig = new XmlClientConfigBuilder(
                    REMOTE_CONFIG_FILE).build( );
        } catch (IOException e) {
            System.err.println("fatal error: unable to load configuration " +
                    "file for remote IMDG instance " +
                    REMOTE_CONFIG_FILE.getAbsolutePath( ));
            System.exit(127);
        }

        return remoteInstanceConfig;
    }
}
