package org.hazelcast.iot_jet_glue;

import org.hazelcast.model.Position;
import org.json.simple.JSONObject;

public class PositionFormatter
{
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

        return resultObj.toJSONString( );
    }
}
