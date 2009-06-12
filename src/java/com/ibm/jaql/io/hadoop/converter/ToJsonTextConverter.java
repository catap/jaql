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
package com.ibm.jaql.io.hadoop.converter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public class ToJsonTextConverter extends JsonToHadoopRecord<WritableComparable<?>, Text>
{
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createKeyConverter()
   */
  @Override
  protected FromJson<WritableComparable<?>> createKeyConverter()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createValConverter()
   */
  @Override
  protected FromJson<Text> createValueConverter()
  {
    return new FromJson<Text>()
    {
      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.converter.ToItem#convert(com.ibm.jaql.json.type.Item,
       *      java.lang.Object)
       */
      public Text convert(JsonValue src, Text tgt)
      {
        ByteArrayOutputStream bstr = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bstr);
        try
        {
          //JsonUtil.printQuoted(out, src.toJSON());
          JsonValue.print(out, src);
          out.flush();
          out.close();
          bstr.close();
        }
        catch (Exception e)
        {
          throw new RuntimeException(e);
        }
        String s = new String(bstr.toByteArray());
        s = s.replace("\r", "");
        s = s.replace("\n", "");
        tgt.set(s);
        return tgt;
      }

      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.converter.ToItem#createTarget()
       */
      public Text createInitialTarget()
      {
        return new Text();
      }
    };
  }
}
