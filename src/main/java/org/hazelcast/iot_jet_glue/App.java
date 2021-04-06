package org.hazelcast.iot_jet_glue;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.hazelcast.model.Position;

import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.entry;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;


public class App 
{
    public static void main(String[ ] args)
    {
        // Determine whether we write to a console logger or to a Kafka
        // stream. Default to writing to a console logger.
        SinkType sinkType = SinkType.fromProperties(
                "resources/pipeline-config.properties", "positions.sink")
                                    .orElse(SinkType.LOGGER);

        // Create the appropriate sink. If we need to create a Kafka sink, we
        // need to read in the properties file from the
        Sink<Map.Entry<Long, String>> pipelineSink = null;
        if (sinkType == SinkType.LOGGER) {
            pipelineSink = Sinks.logger( );
        } else if (sinkType == SinkType.KAFKA) {
            pipelineSink = KafkaSinkFactory.create("positions");
        } else {
            System.err.println("unsupported sink type: " + sinkType);
            System.exit(127);
        }

        JetInstance embeddedInstance = JetFactory.create( );

        Pipeline p = Pipeline.create( );
        p.readFrom(Sources.<Long, Position>remoteMapJournal(
                "org.hazelcast.model.Position",
                RemoteIMDGConfigFactory.createConfig( ), START_FROM_OLDEST))
         .withoutTimestamps( )
         .map(positionEntry -> entry(positionEntry.getKey( ),
                 PositionFormatter.format(positionEntry.getValue( ))))
         .writeTo(pipelineSink);

        embeddedInstance.newJob(p).join( );
    }
}
