/*
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
package com.ibm.jaql.lang.expr.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;


public class BuildLuceneFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par34
  {
    public Descriptor()
    {
      super("buildLucene", BuildLuceneFn.class);
    }
  }
  
  private BinaryFullSerializer serializer = DefaultBinaryFullSerializer.getInstance();
  
  public BuildLuceneFn(Expr[] exprs)
  {
    super(exprs);
  }

  public BuildLuceneFn(Expr input, Expr fileDesc, Expr fieldFn)
  {
    super(input, fileDesc, fieldFn);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonRecord fd = (JsonRecord)exprs[1].eval(context);
    if( fd == null )
    {
      return null;
    }
    JsonString loc = (JsonString)fd.get(new JsonString("location"));
    if( loc == null )
    {
      return null;
    }
    Function keyFn = (Function)exprs[2].eval(context);
    if( keyFn == null )
    {
      return null;
    }
    Function valFn = (Function)exprs[3].eval(context);
    JsonIterator iter = exprs[0].iter(context);
    JsonValue[] fnArgs = new JsonValue[1];
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(loc.toString(), analyzer, true);
    ByteArrayOutputStream buf = null;
    DataOutputStream out = null;
    if( valFn != null )
    {
      buf = new ByteArrayOutputStream();
      out = new DataOutputStream(buf);
    }

    for (JsonValue value : iter)
    {
      fnArgs[0] = value;
      keyFn.setArguments(fnArgs);
      JsonIterator keyIter = keyFn.iter(context);
      Document doc = null;
      for (JsonValue key : keyIter)
      {
        JsonString jkey = (JsonString)key;
        if( doc == null )
        {
          doc = new Document();
        }
        doc.add(new Field("key", jkey.toString(), Store.NO, Index.UN_TOKENIZED)); // TODO: typed keys, store binary value
      }
      
      if( doc != null )
      {
        if( valFn != null )
        {
          valFn.setArguments(fnArgs);
          JsonIterator valIter = valFn.iter(context);
          for (JsonValue val : valIter)
          {
            JsonRecord jrec = (JsonRecord)val;
            for (Entry<JsonString, JsonValue> e : jrec)
            {
              JsonString name = e.getKey();
              JsonValue fval = e.getValue();
              buf.reset();
              serializer.write(out, fval);
              out.flush();
              byte[] bytes = buf.toByteArray();
              doc.add(new Field(name.toString(), bytes, Store.COMPRESS));
            }
          }
        }
        writer.addDocument(doc);
      }
    }
        
    writer.optimize();
    writer.close();
    return fd;
  }

}
