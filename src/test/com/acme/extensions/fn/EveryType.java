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

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.JaqlFunction;

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
      JsonBool         bool = new JsonBool();
      JsonString       str  = new JsonString();
      JsonBinary       bin  = new JsonBinary();
      JsonDate         date = new JsonDate();
      JaqlFunction     fn   = new JaqlFunction();

      // These are encodings of JArray
      SpilledJsonArray   arrs = new SpilledJsonArray();
      BufferedJsonArray   arrf = new BufferedJsonArray();

      // These are encodings of JNumber
      JsonDecimal      dec  = new JsonDecimal();
      JsonLong         lng  = new JsonLong();

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
            arrf.set(i, a.get(n - 1 - i));
          }
          currentValue = arrf;
        }
        else if (v instanceof JsonRecord)
        {
          // Add "my_" before all the names of the record
          JsonRecord r = (JsonRecord) v;
          rec.clear();
          for (int i = 0; i < r.arity(); i++)
          {
            JsonString oldName = r.getName(i);
            v = r.getValue(i);
            String nm = "my_" + oldName;
            rec.add(nm, v);
          }
          currentValue = rec;
        }
        else if (v instanceof JsonBool)
        {
          // Negate bools
          JsonBool b = (JsonBool) v;
          bool.setValue(!b.getValue());
          currentValue = bool;
        }
        else if (v instanceof JsonString)
        {
          // Add "hi, " before JStrings
          JsonString s = (JsonString) v;
          str.set("hi, " + s);
          currentValue = str;
        }
        else if (v instanceof JsonNumber)
        {
          // Set a number to the negated or floor the value (and muck with encodings)
          JsonNumber n = (JsonNumber) v;
          if (n instanceof JsonDecimal)
          {
            lng.setValue(n.longValue());
            currentValue = lng;
          }
          else if (n instanceof JsonLong)
          {
            dec.setValue(-n.longValue());
            currentValue = dec;
          }
        }
        else if (v instanceof JsonBinary)
        {
          // Add 0xABADDEED before JBinary
          JsonBinary b = (JsonBinary) v;
          byte[] bytes1 = b.getInternalBytes(); // Don't modify this!  v, b, bytes1 are not ours!
          int n = b.getLength();
          bin.ensureCapacity(n + 4);
          byte[] bytes2 = bin.getInternalBytes(); // This can be modifed because bin is ours.
          bytes2[0] = (byte) 0xAB;
          bytes2[1] = (byte) 0xAD;
          bytes2[2] = (byte) 0xDE;
          bytes2[3] = (byte) 0xED;
          System.arraycopy(bytes1, 0, bytes2, 4, n);
          bin.setBytes(bytes2, n + 4);
          currentValue = bin;
        }
        else if (v instanceof JsonDate)
        {
          // Add one hour to the date
          JsonDate d = (JsonDate) v;
          date.setMillis(d.getMillis() + 60 * 60 * 1000);
          currentValue = date;
        }
        else if (v instanceof JaqlFunction)
        {
          // TODO: Java functions need the context to be evaluated, and take Item arguments...
          JaqlFunction f = (JaqlFunction) v;
          fn.setCopy(f);
          currentValue = f;
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
