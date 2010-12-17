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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Return the fields of a raw FileSplit.
 * 
 * fileSplitToRecord( split: { class: string, split: binary, * } ):
 *   { path: string, start: long, length: long, locations: [string...] }
 *
 */
public class FileSplitToRecordFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("fileSplitToRecord", FileSplitToRecordFn.class);
    }
  }

  public static final JsonString[] NAMES = new JsonString[] {
    new JsonString("path"),
    new JsonString("start"),
    new JsonString("length"),
    new JsonString("locations")
  };
  
  protected DataInputBuffer in;
  protected MutableJsonString jpath;
  protected MutableJsonLong jstart;
  protected MutableJsonLong jlength;
  protected BufferedJsonArray jlocations;
  protected BufferedJsonRecord resultRec;
  protected JsonValue[] values;
  
  public FileSplitToRecordFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  protected JsonRecord evalRaw(Context context) throws Exception
  {
    // { path: string, start: long, length: long, locations: [string...] }
    if( in == null )
    {
      in = new DataInputBuffer();
      jpath = new MutableJsonString();
      jstart = new MutableJsonLong();
      jlength = new MutableJsonLong();
      jlocations = new BufferedJsonArray();
      values = new JsonValue[] {
          jpath,
          jstart,
          jlength,
          jlocations
      };
      resultRec = new BufferedJsonRecord();
      resultRec.set(NAMES, values, NAMES.length);
    }

    JsonRecord splitRec = (JsonRecord)exprs[0].eval(context);

    JsonString jsplitClassName = (JsonString)splitRec.get(InputSplitsFn.CLASS_TAG);
    Class<? extends FileSplit> splitCls = 
      (Class<? extends FileSplit>)ClassLoaderMgr.resolveClass(jsplitClassName.toString());
    FileSplit split = (FileSplit)ReflectionUtils.newInstance(splitCls, null);
    JsonBinary rawSplit = (JsonBinary)splitRec.get(InputSplitsFn.SPLIT_TAG);
    in.reset(rawSplit.getInternalBytes(), rawSplit.bytesOffset(), rawSplit.bytesLength());
    split.readFields(in);
    JsonArray jlocs = (JsonArray)splitRec.get( InputSplitsFn.LOCATIONS_TAG );
    
    jpath.setCopy( split.getPath().toString() );
    jstart.set( split.getStart() );
    jlength.set( split.getLength() );
    if( jlocs != null )
    {
      values[3] = jlocs;
    }
    else
    {
      String[] locs = split.getLocations();
      jlocations.resize(locs.length);
      for(int i = 0 ; i < locs.length ; i++)
      {
        jlocations.set(i, new JsonString(locs[i]));
      }
      values[3] = jlocations;
    }
    
    return resultRec;
  }
}
