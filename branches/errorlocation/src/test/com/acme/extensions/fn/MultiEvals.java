package com.acme.extensions.fn;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class MultiEvals {
    
    //get v's type
    public JsonString eval(JsonValue v){
        return new JsonString(v.getType().getName());
    }
    
    //check if type of v1 and v2 are same
    public JsonBool eval(JsonValue v1, JsonValue v2){
       if( v1.getType().equals(v2.getType()) ){
           return JsonBool.TRUE;
       }else{
           return JsonBool.FALSE;
       }
       
    }

    //add 3 longs
    public JsonLong eval(JsonLong a, JsonLong b, JsonLong c){
        long r = a.longValue() + b.longValue() + c.longValue();
        return new JsonLong(r);
    }
}
