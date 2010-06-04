/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.io;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Take a i/o descriptor and return a list of raw splits:
 *    [{ class: string, split: binary, locations: [string...] }...]   
 */
public class InputSplitsFn extends IterExpr
{
  public static final JsonString CLASS_TAG = new JsonString("class");
  public static final JsonString SPLIT_TAG = new JsonString("split");
  public static final JsonString LOCATIONS_TAG = new JsonString("locations");
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("inputSplits", InputSplitsFn.class);
    }
  }

  public InputSplitsFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    JsonValue iod = exprs[0].eval(context);

    Adapter adapter = JaqlUtil.getAdapterStore().input.getAdapter(iod);
    if( !(adapter instanceof HadoopInputAdapter) )
    {
      throw new ClassCastException("i/o descriptor must be for an input format");
    }
    HadoopInputAdapter hia = (HadoopInputAdapter)adapter;
    JobConf conf = new JobConf(); // TODO: allow configuration
    hia.setParallel(conf); // right thing to do?
    hia.configure(conf); // right thing to do?
    int numSplits = conf.getNumMapTasks(); // TODO: allow override
    final InputSplit[] splits = hia.getSplits(conf, numSplits);
    final MutableJsonString className = new MutableJsonString();
    final MutableJsonBinary rawSplit = new MutableJsonBinary();
    final BufferedJsonRecord rec = new BufferedJsonRecord(3);
    final BufferedJsonArray locArr = new BufferedJsonArray();
    rec.add(CLASS_TAG, className);
    rec.add(SPLIT_TAG, rawSplit);    
    rec.add(LOCATIONS_TAG, locArr);    
    
    return new JsonIterator(rec)
    {
      DataOutputBuffer out = new DataOutputBuffer();
      int i = 0;
      
      @Override
      public boolean moveNext() throws Exception
      {
        if( i >= splits.length )
        {
          return false;
        }
        InputSplit split = splits[i++];
        className.setCopy(split.getClass().getCanonicalName());
        out.reset();
        split.write(out);
        rawSplit.setCopy(out.getData(), out.getLength());
        locArr.clear();
        for( String loc: split.getLocations() )
        {
          locArr.add(new JsonString(loc));
        }
        return true;
      }
    };
  }
}
