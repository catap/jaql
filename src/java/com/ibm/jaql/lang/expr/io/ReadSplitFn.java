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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.hadoop.RecordReaderValueIter;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * An expression used for reading data from a single split into jaql.
 */
public class ReadSplitFn extends AbstractReadExpr 
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22 {
    public Descriptor() {
      super("readSplit", ReadSplitFn.class);
    }
  }

  public ReadSplitFn(Expr... exprs)
  {
    super(exprs);
  }
  
  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    // Close the previous adapter, if still open:
    if( adapter != null )
    {
      adapter.close();
      adapter = null;
    }
    
    // evaluate the arguments
    JsonValue args = exprs[0].eval(context);
    JsonRecord splitRec = (JsonRecord)exprs[1].eval(context);
  
    if( splitRec == null )
    {
      return JsonIterator.EMPTY;
    }
    
    // get the InputAdapter according to the type
    HadoopInputAdapter hia = (HadoopInputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(args);
    adapter = hia;
    JobConf conf = new JobConf(); // TODO: allow configuration
    hia.setParallel(conf); // right thing to do?
    
    JsonString jsplitClassName = (JsonString)splitRec.get(InputSplitsFn.CLASS_TAG);
    Class<? extends InputSplit> splitCls = 
      (Class<? extends InputSplit>)ClassLoaderMgr.resolveClass(jsplitClassName.toString());
    InputSplit split = (InputSplit)ReflectionUtils.newInstance(splitCls, conf);
    
    DataInputBuffer in = new DataInputBuffer();
    JsonBinary rawSplit = (JsonBinary)splitRec.get(InputSplitsFn.SPLIT_TAG);
    in.reset(rawSplit.getInternalBytes(), rawSplit.bytesOffset(), rawSplit.bytesLength());
    split.readFields(in);
    
    RecordReader<JsonHolder, JsonHolder> rr = hia.getRecordReader(split, conf, Reporter.NULL);
    return new RecordReaderValueIter(rr);
  }
}
