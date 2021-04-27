package org.hazelcast.iot_jet_glue;

import org.hazelcast.model.Event;
import org.json.simple.JSONObject;

import java.util.logging.Logger;

public class EventFormatter
{
    private static final Logger LOG = Logger.getLogger(
            "org.hazelcast" + ".iot_jet_glue.EventFormatter");
    @SuppressWarnings("unchecked")
    public static String format(Event event)
    {
        JSONObject resultObj = new JSONObject( );

        resultObj.put("ObjectKind", "Event");
        resultObj.put("id", event.getId( ));
        resultObj.put("eventType", event.getType( ));
        resultObj.put("deviceId", event.getDeviceId( ));
        resultObj.put("serverTime", event.getServerTime( ).toString( ));
        resultObj.put("positionId", event.getPositionId( ));
        resultObj.put("geofenceId", event.getGeofenceId( ));

        String result = resultObj.toJSONString( );
        LOG.info("formatted Event object as JSON = " + result);
        return result;
    }
}
