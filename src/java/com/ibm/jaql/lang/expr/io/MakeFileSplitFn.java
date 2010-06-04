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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.FileSplit;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Constructs a raw FileSplit.  You should know what you're doing if you're using this!
 * 
 * makeFileSplit( file: string, start: long, length: long, hosts: [string...] ):
 *   { class: string, split: binary, locations:[string...] }
 *
 */
public class MakeFileSplitFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par44
  {
    public Descriptor()
    {
      super("makeFileSplit", MakeFileSplitFn.class);
    }
  }

  public static final JsonString[] NAMES = new JsonString[] {
    InputSplitsFn.CLASS_TAG,
    InputSplitsFn.SPLIT_TAG,
    InputSplitsFn.LOCATIONS_TAG
  };


  // runtime state
  protected DataOutputBuffer out;
  protected MutableJsonString className;
  protected MutableJsonBinary rawSplit;
  protected BufferedJsonRecord resultRec;
  protected JsonValue[] values;
  
  
  public MakeFileSplitFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonRecord eval(Context context) throws Exception
  {
    if( out == null )
    {
      out = new DataOutputBuffer();
      className = new MutableJsonString();
      rawSplit = new MutableJsonBinary();
      values = new JsonValue[] {
          className,
          rawSplit,
          null
      };
      resultRec = new BufferedJsonRecord();
      resultRec.set(NAMES, values, NAMES.length);
    }
    
    JsonString jfile   = (JsonString)exprs[0].eval(context);
    JsonNumber jstart  = (JsonNumber)exprs[1].eval(context);
    JsonNumber jlength = (JsonNumber)exprs[2].eval(context);
    JsonArray  jhosts  = (JsonArray) exprs[3].eval(context);

    String file = jfile.toString();
    long start  = jstart.longValueExact();
    long length = jlength.longValueExact();
    if( jhosts == null ) jhosts = JsonArray.EMPTY;
    String[] hosts = new String[(int)jhosts.count()];
    JsonIterator iter = jhosts.iter();
    for(int i = 0 ; i < hosts.length ; i++)
    {
      iter.moveNext();
      JsonString jhost = (JsonString)iter.current();
      hosts[i] = jhost.toString();
    }
    
    FileSplit split = new FileSplit(new Path(file), start, length, hosts);
    className.setCopy( split.getClass().getCanonicalName() );
    out.reset();
    split.write(out);
    rawSplit.set(out.getData(), out.getLength());
    values[2] = jhosts;
    
    return resultRec;
  }
}
