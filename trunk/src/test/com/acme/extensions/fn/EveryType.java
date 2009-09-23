/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.acme.extensions.fn;

import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.json.type.MutableJsonBool;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.expr.function.Function;

/**
 * 
 */
public class EveryType
{
  /**
   * @param vals
   * @return
   * @throws Exception
   */
  public JsonIterator eval(final JsonIterator vals) throws Exception
  {
    if (vals == null)
    {
      return null;
    }

    return new JsonIterator() {
      // These are both types and encodings
      BufferedJsonRecord rec  = new BufferedJsonRecord();
      MutableJsonBool       bool = new MutableJsonBool();
      MutableJsonString       str  = new MutableJsonString();
      MutableJsonBinary       bin  = new MutableJsonBinary();
      MutableJsonDate         date = new MutableJsonDate();
      Function     fn;

      // These are encodings of JArray
      SpilledJsonArray   arrs = new SpilledJsonArray();
      BufferedJsonArray   arrf = new BufferedJsonArray();

      // These are encodings of JNumberic
      MutableJsonDecimal      dec  = new MutableJsonDecimal();
      MutableJsonLong         lng  = new MutableJsonLong();
      MutableJsonDouble dbl = new MutableJsonDouble();

      public boolean moveNext() throws Exception
      {
        if (!vals.moveNext())
        {
          return false;
        }
        JsonValue v = vals.current();
        if (v == null)
        {
          // Just return nulls
          currentValue = null;
        }
        else if (v instanceof SpilledJsonArray)
        {
          // Keep every other entry of the array
          SpilledJsonArray a = (SpilledJsonArray) v;
          arrs.clear();
          JsonIterator iter = a.iter();
          boolean addit = true;
          while (iter.moveNext())
          {
            if (addit)
            {
              arrs.addCopy(iter.current());
            }
            addit = !addit;
          }
          currentValue = arrs;
        }
        else if (v instanceof BufferedJsonArray)
        {
          // Reverse the array
          BufferedJsonArray a = (BufferedJsonArray) v;
          int n = a.size();
          arrf.resize(n);
          for (int i = 0; i < n; i++)
          {
            arrf.set(i, a.getUnchecked(n - 1 - i));
          }
          currentValue = arrf;
        }
        else if (v instanceof JsonRecord)
        {
          // Add "my_" before all the names of the record
          JsonRecord r = (JsonRecord) v;
          rec.clear();
          for(Entry<JsonString, JsonValue> e : r)
          {
            JsonString oldName = e.getKey();
            v = e.getValue();
            String nm = "my_" + oldName;
            rec.add(new JsonString(nm), v);
          }
          currentValue = rec;
        }
        else if (v instanceof JsonBool)
        {
          // Negate bools
          JsonBool b = (JsonBool) v;
          bool.set(!b.get());
          currentValue = bool;
        }
        else if (v instanceof JsonString)
        {
          // Add "hi, " before JStrings
          JsonString s = (JsonString) v;
          str.setCopy("hi, " + s);
          currentValue = str;
        }
        else if (v instanceof JsonNumber)
        {
          // Set a number to the negated or floor the value (and muck with encodings)
          JsonNumber n = (JsonNumber) v;
          if (n instanceof JsonDecimal)
          {
            lng.set(n.longValue());
            currentValue = lng;
          }
          else if (n instanceof JsonLong)
          {
            dec.set(-n.longValue());
            currentValue = dec;
          }
          else if (n instanceof JsonDouble)
          {
            dbl.set(-n.longValue());
            currentValue = dbl;
          }
        }
        else if (v instanceof JsonBinary)
        {
          // Add 0xABADDEED before JBinary
          JsonBinary b = (JsonBinary) v;
          int n = b.bytesLength();
          bin.ensureCapacity(n + 4);
          byte[] bytes2 = bin.get(); // This can be modifed because bin is ours.
          bytes2[0] = (byte) 0xAB;
          bytes2[1] = (byte) 0xAD;
          bytes2[2] = (byte) 0xDE;
          bytes2[3] = (byte) 0xED;
          b.writeBytes(0, bytes2, 4, n);
          bin.set(bytes2, n + 4);
          currentValue = bin;
        }
        else if (v instanceof JsonDate)
        {
          // Add one hour to the date
          JsonDate d = (JsonDate) v;
          date.set(d.get() + 60 * 60 * 1000);
          currentValue = date;
        }
        else if (v instanceof Function)
        {
          // TODO: Java functions need the context to be evaluated, and take Item arguments...
          Function f = (Function) v;
          fn = f.getCopy(null);
          currentValue = fn;
        }
        else
        {
          throw new RuntimeException("unknown type: " + v.getClass().getName());
        }
        return true;
      }
    };
  }
}
