/**
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.io.hadoop;

import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.hadoop.converter.KeyValueExport;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/**
 *
 */
public class ToLinesConverter implements KeyValueExport<NullWritable, Text> {
  
  private String nullString;
  
  @Override
  public void convert(JsonValue src, NullWritable key, Text val) {
    try {
      if (src == null)
      {
        val.set(nullString);
      } 
      else if (src instanceof JsonString)
      {
        JsonString jstr = (JsonString)src;
        // TODO: There is a potential bug if jstr contains embedded newlines, but
        // that is a problem for TextOutputFormat in general.  For now, we will
        // silently replace them with space...
        if( jstr.indexOf('\n') >= 0 )
        {
          byte[] buf = jstr.getCopy();
          for(int i = 0 ; i < buf.length ; i++)
          {
            if( buf[i] == '\n' )
            {
              buf[i] = ' ';
            }
          }
          val.set(buf);
        }
        else
        {
          val.set(jstr.getInternalBytes(), jstr.bytesOffset(), jstr.bytesLength());
        }
      }
      else if (src instanceof JsonAtom)
      {
        val.set(JsonUtil.printToString(src));
      }
      else
      {
        throw new ClassCastException("Only convert atomic types using lines()");
      }
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e);
    }
  }

  @Override
  public NullWritable createKeyTarget() {
    return null;
  }

  @Override
  public Text createValueTarget() {
    return new Text();
  }

  @Override
  public void init(JsonRecord options) {
    nullString = System.getProperty("text.nullstring", "");
  }

}
