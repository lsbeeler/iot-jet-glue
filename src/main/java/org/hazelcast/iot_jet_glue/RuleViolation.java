package org.hazelcast.iot_jet_glue;

import org.hazelcast.model.Event;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class RuleViolation
{
    private final String violationDescription;
    private final Event[ ] causativeEvents;

    public RuleViolation(String violationDescription, Event[ ] causativeEvents)
    {
        this.violationDescription = violationDescription;
        this.causativeEvents = causativeEvents;
    }

    public String getJSONKey( )
    {
        Event cause = causativeEvents[causativeEvents.length - 1];
        return cause.getDeviceId( ) + "-" + cause.getServerTime( ).toString( );
    }

    public String getJSONValue( )
    {
        JSONObject resultObj = new JSONObject( );

        Event cause = causativeEvents[causativeEvents.length - 1];

        resultObj.put("ObjectKind", "RuleViolation");
        resultObj.put("deviceId", cause.getDeviceId( ));
        resultObj.put("serverTime", cause.getServerTime( ));
        resultObj.put("description", violationDescription);

        JSONArray rawEvents = new JSONArray( );
        for (Event e : causativeEvents)
            rawEvents.add(e.getId( ));

        resultObj.put("rawEvents", rawEvents);

        return resultObj.toJSONString( );
    }
}
