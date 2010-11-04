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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrintBuffer;

/**
 * 
 */
public class ToJsonTextConverter extends JsonToHadoopRecord<WritableComparable<?>, Text>
{

  @Override
  protected FromJson<WritableComparable<?>> createKeyConverter()
  {
    return null;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.converter.ItemToHadoopRecord#createKeyTarget()
   */
  @Override
  public WritableComparable<?> createKeyTarget() 
  {
    return NullWritable.get();
  };

  @Override
  protected FromJson<Text> createValueConverter()
  {
    return new FromJson<Text>()
    {
      public Text convert(JsonValue src, Text tgt)
      {
        FastPrintBuffer out = new FastPrintBuffer();
        try
        {
          //JsonUtil.printQuoted(out, src.toJSON());
          JsonUtil.print(out, src);
          out.close();
        }
        catch (Exception e)
        {
          throw new RuntimeException(e);
        }
        String s = out.toString();
        s = s.replace("\r", "");
        s = s.replace("\n", "");
        tgt.set(s);
        return tgt;
      }

      public Text createTarget()
      {
        return new Text();
      }
    };
  }
}
