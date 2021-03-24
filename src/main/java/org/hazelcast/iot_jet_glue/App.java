package org.hazelcast.iot_jet_glue;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.hazelcast.model.Position;

import java.util.Map.Entry;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;


public class App 
{
    public static void main(String[ ] args)
    {
        JetInstance embeddedInstance = JetFactory.createJetInstance( );

        Pipeline p = Pipeline.create( );
        p.readFrom(Sources.<Long, Position>remoteMapJournal(
                "org.hazelcast.model.Position",
                RemoteIMDGConfigFactory.createConfig( ), START_FROM_OLDEST))
         .withoutTimestamps( )
         .map(Entry::getValue)
         .map(PositionFormatter::format)
         .writeTo(Sinks.logger( ));

        embeddedInstance.newJob(p).join( );
    }
}
