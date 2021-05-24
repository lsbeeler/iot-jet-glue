package org.hazelcast.iot_jet_glue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.kafka.clients.producer.Producer;
import org.hazelcast.model.Event;
import org.hazelcast.model.Position;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.util.MapUtil.entry;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;


public class App 
{
    public static JetInstance embeddedInstance = JetFactory.create( );

    public static void main(String[ ] args)
    {
        Sink<Map.Entry<Long, String>> positionsSink = SinkFactory.create(
                EntityType.POSITION, "resources/pipeline-config.properties");
        Pipeline positionsPipeline = Pipeline.create( );
        positionsPipeline.readFrom(Sources.<Long, Position>remoteMapJournal(
                "org.hazelcast.model.Position",
                RemoteIMDGConfigFactory.createConfig( ), START_FROM_OLDEST))
         .withTimestamps(pos -> pos.getValue( ).getDeviceTime( ).getTime( ),
                 30_000)
         .map(positionEntry -> entry(positionEntry.getKey( ),
                 PositionFormatter.format(positionEntry.getValue( ))))
         .writeTo(positionsSink);

        Sink<Map.Entry<Long, String>> eventsSink = SinkFactory.create(
                EntityType.EVENT, "resources/pipeline-config.properties");
        Pipeline eventsPipeline = Pipeline.create( );
        eventsPipeline.readFrom(Sources.<Long, Event>remoteMapJournal(
                "org.hazelcast.model.Event",
                RemoteIMDGConfigFactory.createConfig( ), START_FROM_OLDEST))
         .withTimestamps(evt -> evt.getValue( ).getServerTime( ).getTime( ),
                 30_000)
         .map(eventEntry -> entry(eventEntry.getKey( ),
                 EventRuleEngine.apply(eventEntry.getValue( ))))
         .map(eventEntry -> entry(eventEntry.getKey( ),
                 EventFormatter.format(eventEntry.getValue( ))))
         .writeTo(eventsSink);

        embeddedInstance.newJob(eventsPipeline);
        embeddedInstance.newJob(positionsPipeline).join( );
    }

    private static Producer<String, String> rulesProducer = null;

    public static Producer<String, String> getRulesProducer( )
    {
        if (rulesProducer == null) {
            Properties kafkaProps = KafkaUtils.loadPropertiesForStream(
                    EntityType.RULE);
            rulesProducer = KafkaUtils.createArbitraryProducer(EntityType.RULE,
                            kafkaProps);
        }

        return rulesProducer;
    }
}
