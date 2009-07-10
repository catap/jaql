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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "probeLucene", minArgs = 2, maxArgs = 3)
public class ProbeLuceneFn extends IterExpr
{
  private BinaryFullSerializer serializer = DefaultBinaryFullSerializer.getInstance();
  
  public ProbeLuceneFn(Expr[] exprs)
  {
    super(exprs);
  }

  public ProbeLuceneFn(Expr fileDesc, Expr query, Expr fields)
  {
    super(fileDesc, query, fields);
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    JsonRecord fd = (JsonRecord)exprs[0].eval(context);
    if( fd == null )
    {
      return JsonIterator.NULL;
    }
    JsonString loc = (JsonString)fd.get(new JsonString("location"));
    if( loc == null )
    {
      return JsonIterator.NULL;
    }
    JsonString jquery = (JsonString)exprs[1].eval(context);
    if( jquery == null )
    {
      return JsonIterator.NULL;
    }
    
    HashSet<String> fields = null;
    if( exprs.length > 2 )
    {
      JsonIterator iter = exprs[2].iter(context);
      for (JsonValue sv : iter)
      {
        JsonString s = (JsonString)sv;
        if( s != null )
        {
          if( fields == null )
          {
            fields = new HashSet<String>();
          }
          fields.add(s.toString());
        }
      }
    }
    final FieldSelector fieldSelector = ( fields == null ) ? null
          : new SetBasedFieldSelector(fields, new HashSet<String>());

    final IndexSearcher searcher = new IndexSearcher(loc.toString());
    Analyzer analyzer = new StandardAnalyzer();
    QueryParser qp = new QueryParser("key", analyzer);
    Query query = qp.parse(jquery.toString());

    query = searcher.rewrite(query);
    final Scorer scorer = query.weight(searcher).scorer(searcher.getIndexReader());
    final BufferedJsonRecord rec = new BufferedJsonRecord();
    final JsonString jdoc = new JsonString("doc");
    final JsonLong jdocid = new JsonLong();
    
    return new JsonIterator(rec)
    {
      @Override
      public boolean moveNext() throws Exception
      {
        if( ! scorer.next() )
        {
          return false;
        }
        rec.clear();
        int i = scorer.doc();
        jdocid.set(i);
        rec.add(jdoc, jdocid);
        if( fieldSelector != null )
        {
          Document doc = searcher.doc(i, fieldSelector);
          for( Object x: doc.getFields() )
          {
            Field f = (Field)x;
            String name = f.name();
            byte[] val = f.binaryValue();
            ByteArrayInputStream bais = new ByteArrayInputStream(val); // TODO: reuse
            DataInputStream in = new DataInputStream(bais); // TODO: reuse
            JsonValue ival = serializer.read(in, null);
            rec.add(new JsonString(name), ival);
          }
        }
        return true; // currentValue == rec
      }
    };
  }

}
