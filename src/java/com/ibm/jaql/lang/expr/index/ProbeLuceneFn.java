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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "probeLucene", minArgs = 2, maxArgs = 3)
public class ProbeLuceneFn extends IterExpr
{
  public ProbeLuceneFn(Expr[] exprs)
  {
    super(exprs);
  }

  public ProbeLuceneFn(Expr fileDesc, Expr query, Expr fields)
  {
    super(fileDesc, query, fields);
  }

  @Override
  public Iter iter(Context context) throws Exception
  {
    Item item = exprs[0].eval(context);
    JRecord fd = (JRecord)item.get();
    if( fd == null )
    {
      return Iter.nil;
    }
    JString loc = (JString)fd.getValue("location").get();
    if( loc == null )
    {
      return Iter.nil;
    }
    JString jquery = (JString)exprs[1].eval(context).get();
    if( jquery == null )
    {
      return Iter.nil;
    }
    
    HashSet<String> fields = null;
    if( exprs.length > 2 )
    {
      Iter iter = exprs[2].iter(context);
      while( (item = iter.next()) != null )
      {
        JString s = (JString)item.get();
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
    final MemoryJRecord rec = new MemoryJRecord();
    final Item result = new Item(rec);
    final JString jdoc = new JString("doc");
    final JLong jdocid = new JLong();
    final Item docid = new Item(jdocid);
    
    return new Iter()
    {
      @Override
      public Item next() throws Exception
      {
        if( ! scorer.next() )
        {
          return null;
        }
        rec.clear();
        int i = scorer.doc();
        jdocid.setValue(i);
        rec.add(jdoc, docid);
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
            Item ival = new Item();
            ival.readFields(in);
            rec.add(name, ival);
          }
        }
        return result;
      }
    };
  }

}
