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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.BufferedRandomAccessFile;
import com.ibm.jaql.util.LongArray;


@JaqlFn(fnName = "probeJIndex", minArgs = 2, maxArgs = 2)
public class ProbeJIndexFn extends IterExpr
{
  private int rootLevel;
  private ArrayList<Item> root;
  private LongArray rootp;
  private ArrayList<Index> indexes;
  private JLong joffset = new JLong();
  String loc;
  Item key = new Item();
  Item value = new Item();
  BufferedRandomAccessFile base;
  FixedJArray tuple = new FixedJArray(2);
  Item result = new Item(tuple);
  Item range;
  Item low;
  Item high;
  
  public ProbeJIndexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Iter iter(Context context) throws Exception
  {
    if( base == null )
    {
      Item fdItem = exprs[0].eval(context);
      JRecord fd = (JRecord)fdItem.get();
      if( fd == null )
      {
        return Iter.nil;
      }
      JString jloc = (JString)fd.getValue("location").get();
      if( jloc == null )
      {
        return Iter.nil;
      }
      loc = jloc.toString();

      base = new BufferedRandomAccessFile(loc+".base", "r", 1024); // TODO: how to set buffer?
    }
    
    range = exprs[1].eval(context);
    JRecord jrange = (JRecord)range.get();
    low = jrange.getValue("low", null);
    high = jrange.getValue("high", null);
    if( low == null || low.isNull() )
    {
      base.seek(0);
    }
    else
    {
      if( root == null )
      {
        loadIndexes();
      }
      int p = Collections.binarySearch(root, low);
      if( p < 0 )
      {
        p = -p - 2;
      }
      long offset = (p < 0) ? 0 : rootp.get(p);
      offset = indexLookup(rootLevel - 1, offset);
      base.seek(offset);
      try
      {
        do
        {
          key.readFields(base);
          value.readFields(base);
        }
        while( key.compareTo(low) < 0 );
      }
      catch(EOFException ex)
      {
        return Iter.empty;
      }
    }

    return new Iter()
    {
      boolean atFirst = true;
      
      @Override
      public Item next() throws Exception
      {
        try
        {
          if( !atFirst )
          {
            key.readFields(base);
          }
          if( high == null || key.compareTo(high) <= 0 )
          {
            if( !atFirst )
            {
              value.readFields(base);
            }
            tuple.set(0, key);
            tuple.set(1, value);
            atFirst = false;
            return result;
          }
        }
        catch(EOFException e) {}
        base.seek(base.length()); // just to be safe in case next() is called again
        return null;
      }
    };
  }
    
  private void loadIndexes() throws IOException
  {
    indexes = new ArrayList<Index>();
    root = new ArrayList<Item>();
    rootp = new LongArray();

    int i = 0;
    try
    {
      while(true)
      {
        Index index = new Index(loc, i);
        indexes.add(index);
        i++;
      }
    }
    catch(FileNotFoundException ex) {}    
    rootLevel = i-1;
    Index index = indexes.get(rootLevel);
    try
    {
      while( true )
      {
        Item k = new Item();
        k.readFields(index.in);
        joffset.readFields(index.in);
        root.add(k);
        rootp.add(joffset.value);
      }
    }
    catch( EOFException ex ) {}
  }
  
  private long indexLookup(int i, long offset) throws IOException
  {
    for( ; i >= 0 ; i--)
    {
      Index index = indexes.get(i);
      index.in.seek(offset);
      joffset.value = 0;
      try
      {
        do
        {
          offset = joffset.value;
          key.readFields(index.in);
          joffset.readFields(index.in);
          assert joffset.value >= 0;
        }
        while( key.compareTo(low) < 0 );
      }
      catch( EOFException ex ) {}
    }
    return offset;
  }


  static class Index
  {
    BufferedRandomAccessFile in;
    
    public Index(String loc, int level) throws IOException
    {
      in = new BufferedRandomAccessFile(loc+".idx"+level, "r", 1024); // TODO: how to set buffer?
    }
  }
}
