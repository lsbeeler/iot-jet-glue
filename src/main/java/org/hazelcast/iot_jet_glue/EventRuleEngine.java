package org.hazelcast.iot_jet_glue;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hazelcast.model.Event;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class EventRuleEngine
{
    private static final Logger LOG = Logger.getLogger(
            "org.hazelcast.iot_jet_glue.PositionFormatter");
    private static final long NO_STOP_THRESHOLD_MSEC = 6500;
    private static Map<String, Event> geofenceEntryMap =
            new ConcurrentHashMap<>( );
    private static final String RULE_VIOLATION_TOPIC_NAME = "rules";
    private static Producer<String, String> rulesProducer =
            App.getRulesProducer( );

    public static Event apply(Event event)
    {
        if (!event.getType( ).equals("geofenceEnter") &&
                !event.getType( ).equals("geofenceExit")) {
            return event;
        }

        LOG.info("running business rules on event: " + EventFormatter.format(
                event));

        String k = makeKey(event);
        if (!geofenceEntryMap.containsKey(k)) {
            geofenceEntryMap.put(k, event);
        } else {
            Event cachedEvt = geofenceEntryMap.get(k);

            long entryTime = 0;
            long exitTime = 0;
            boolean validDelta = false;
            Event entryEvent = null;
            Event exitEvent = null;

            if (cachedEvt.getType( ).equals("geofenceEnter")) {
                entryTime = cachedEvt.getServerTime( ).getTime( );
                entryEvent = cachedEvt;
                if (event.getType( ).equals("geofenceExit")) {
                    exitTime = event.getServerTime( ).getTime( );
                    exitEvent = event;
                    validDelta = true;
                }
            } else if (cachedEvt.getType( ).equals("geofenceExit")) {
                exitTime = cachedEvt.getServerTime( ).getTime( );
                exitEvent = cachedEvt;
                if (event.getType( ).equals("geofenceEnter")) {
                    entryTime = event.getServerTime( ).getTime( );
                    entryEvent = event;
                    validDelta = true;
                }
            }

            if (validDelta) {
                long timeDelta = exitTime - entryTime;

                if (timeDelta < 0)
                    return event;

                if (timeDelta < NO_STOP_THRESHOLD_MSEC) {
                    LOG.info("geofence exit - entry delta time of " +
                            timeDelta / 1000 + " seconds is less than no-stop" +
                            " threshold time of " + NO_STOP_THRESHOLD_MSEC /
                            1000 + " seconds.");
                    RuleViolation violation = new RuleViolation(
                            "failedToStop",
                            new Event[ ]{entryEvent, exitEvent});
                    LOG.info("produced business rule violation = " +
                            violation.getJSONValue( ));
                    rulesProducer.send(new ProducerRecord<>(
                            RULE_VIOLATION_TOPIC_NAME,
                            violation.getJSONKey( ),
                            violation.getJSONValue( )));
                } else {
                    LOG.info("geofence exit - entry delta time of " +
                            timeDelta / 1000 + " seconds is within safe stop " +
                            "threshold time of " + NO_STOP_THRESHOLD_MSEC /
                            1000 + " seconds.");
                }

            }
        }

        return event;
    }

    public static String makeKey(Event evt)
    {
        return evt.getDeviceId( ) + "-" + evt.getGeofenceId( );
    }
}
