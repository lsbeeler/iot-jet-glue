package org.hazelcast.iot_jet_glue;

import org.hazelcast.model.Position;
import org.json.simple.JSONObject;

import java.util.logging.Logger;

public class PositionFormatter
{
    private static final Logger LOG = Logger.getLogger(
            "org.hazelcast" + ".iot_jet_glue.PositionFormatter");

    @SuppressWarnings("unchecked")
    public static String format(Position position)
    {
        JSONObject resultObj = new JSONObject( );

        resultObj.put("ObjectKind", "Position");
        resultObj.put("id", position.getId( ));
        resultObj.put("deviceId", position.getDeviceId( ));
        resultObj.put("deviceTime", position.getDeviceTime( ).toString( ));
        resultObj.put("latitude", position.getLatitude( ));
        resultObj.put("longitude", position.getLongitude( ));
        resultObj.put("altitude", position.getAltitude( ));
        resultObj.put("speed", position.getSpeed( ));
        resultObj.put("accuracy", position.getAccuracy( ));
        resultObj.put("course", position.getCourse( ));

        String result = resultObj.toJSONString( );
        LOG.info("formatted Position object as JSON " + result);
        return result;
    }
}
