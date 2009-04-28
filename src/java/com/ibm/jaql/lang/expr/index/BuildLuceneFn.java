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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "buildLucene", minArgs = 3, maxArgs = 4)
public class BuildLuceneFn extends Expr
{
  public BuildLuceneFn(Expr[] exprs)
  {
    super(exprs);
  }

  public BuildLuceneFn(Expr input, Expr fileDesc, Expr fieldFn)
  {
    super(input, fileDesc, fieldFn);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    Item fdItem = exprs[1].eval(context);
    JRecord fd = (JRecord)fdItem.get();
    if( fd == null )
    {
      return Item.NIL;
    }
    JString loc = (JString)fd.getValue("location").get();
    if( loc == null )
    {
      return Item.NIL;
    }
    JFunction keyFn = (JFunction)exprs[2].eval(context).get();
    if( keyFn == null )
    {
      return Item.NIL;
    }
    JFunction valFn = (JFunction)exprs[3].eval(context).get();
    Iter iter = exprs[0].iter(context);
    Item item;
    Item[] fnArgs = new Item[1];
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(loc.toString(), analyzer, true);
    ByteArrayOutputStream buf = null;
    DataOutputStream out = null;
    if( valFn != null )
    {
      buf = new ByteArrayOutputStream();
      out = new DataOutputStream(buf);
    }

    while( (item = iter.next()) != null )
    {
      fnArgs[0] = item;
      Iter keyIter = keyFn.iter(context, fnArgs);
      Item key;
      Document doc = null;
      while( (key = keyIter.next()) != null )
      {
        JString jkey = (JString)key.get();
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
          Iter valIter = valFn.iter(context, fnArgs);
          Item val;
          while( (val = valIter.next()) != null )
          {
            JRecord jrec = (JRecord)val.get();
            int n = jrec.arity();
            for( int i = 0 ; i < n ; i++ )
            {
              JString name = jrec.getName(i);
              Item fval = jrec.getValue(i);
              buf.reset();
              fval.write(out);
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
    return fdItem;
  }

}
