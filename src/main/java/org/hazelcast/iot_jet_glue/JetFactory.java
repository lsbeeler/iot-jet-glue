package org.hazelcast.iot_jet_glue;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

import java.io.File;
import java.io.FileNotFoundException;

public class JetFactory
{
    private static final String IMDG_CONFIG_FILE_PATH =
            "resources/embedded-imdg.xml";
    private static final File IMDG_CONFIG_FILE =
            new File(IMDG_CONFIG_FILE_PATH);

    public static JetInstance create( )
    {
        XmlConfigBuilder imdgConfigBuilder = null;
        try {
            imdgConfigBuilder =
                    new XmlConfigBuilder(IMDG_CONFIG_FILE.getAbsolutePath( ));
        } catch (FileNotFoundException e) {
            System.err.println("fatal error: unable to load configuration " +
                    "file " + IMDG_CONFIG_FILE.getAbsolutePath( ));
            System.exit(127);
        }

        Config imdgConfig = imdgConfigBuilder.build( );

        JetConfig jetConfig = new JetConfig( );
        jetConfig.setHazelcastConfig(imdgConfig);

        return Jet.newJetInstance(jetConfig);
    }
}
