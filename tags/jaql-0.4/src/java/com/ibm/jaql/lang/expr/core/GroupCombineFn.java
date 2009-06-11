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
package com.ibm.jaql.lang.expr.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.PriorityQueue;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.util.ItemHashtable;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.BufferedRandomAccessFile;
import com.ibm.jaql.util.LongArray;

/**
 * groupCombine(input $X, initialFn, partialFn, finalFn) => $Y
 *    initialFn = fn($k,$X) e1 => $P
 *    partialFn = fn($k,$P) => $P
 *    finalFn = fn($k,$P) => $Y
 */
@JaqlFn(fnName="groupCombine", minArgs=4, maxArgs=4)
public class GroupCombineFn extends IterExpr
{
  public static final long memoryLimit = 32 * 1024 * 1024;  // TODO: make configurable
  public static final long keyLimit    =  1 * 1024 * 1024;  // TODO: make configurable
  protected ItemHashtable initialHT;
  protected ItemHashtable partialHT;
  protected Item[] pair = new Item[2];
  protected JFunction initialFn;
  protected JFunction partialFn;
  protected JFunction finalFn;
  protected File spillFileHandle;
  protected RandomAccessFile spillFile;
  protected LongArray spillOffsets;
  
  /**
   * @param exprs
   */
  public GroupCombineFn(Expr[] exprs)
  {
    super(exprs);
  }


  public GroupCombineFn(Expr input, Expr initialFn, Expr partialFn, Expr finalFn)
  {
    super(new Expr[]{input, initialFn, partialFn, finalFn});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  public Expr input()       { return exprs[0]; }
  public Expr initialExpr() { return exprs[1]; }
  public Expr partialExpr() { return exprs[2]; }
  public Expr finalExpr()   { return exprs[3]; }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    initialFn = getFunction(context, initialExpr());
    partialFn = getFunction(context, partialExpr());
    finalFn   = getFunction(context, finalExpr());
    
    Item item;
    boolean madePartials = false;
    initialHT = new ItemHashtable(1); // TODO: add comparator support to ItemHashtable
    partialHT = new ItemHashtable(1); // TODO: add comparator support to ItemHashtable
    Iter iter = input().iter(context);
    while ((item = iter.next()) != null)
    {
      JArray pairArr = (JArray)item.getNonNull();
      pairArr.getTuple(pair);
      initialHT.add(0, pair[0], pair[1]);
      if( initialHT.getMemoryUsage() >= memoryLimit || initialHT.numKeys() >= keyLimit )
      {
        madePartials = true;
        spillInitial(context);
      }
    }
    
    if( ! madePartials )
    {
      return aggInitial(context);
    }
    
    spillInitial(context);
    if( spillFile == null )
    {
      return aggPartial(context);
    }
    
    spillPartial(context);
    return aggSpill(context);
  }


  private JFunction getFunction(Context context, Expr expr) throws Exception
  {
    JFunction f = (JFunction)expr.eval(context).getNonNull();
    if( f.getNumParameters() != 2 )
    {
      throw new RuntimeException("function must have two parameters: "+f);
    }
    return f;
  }


  protected void spillInitial(Context context) throws Exception
  {
    ItemHashtable.Iterator tempIter = initialHT.iter();
    while( tempIter.next() )
    {
      Item item;
      Item key = tempIter.key();
      Iter iter = initialFn.iter(context, key, tempIter.values(0));
      while( (item = iter.next()) != null )
      {
        partialHT.add(0, key, item);
        if( partialHT.getMemoryUsage() >= memoryLimit || partialHT.numKeys() >= keyLimit )
        {
          spillPartial(context);
        }
      }
    }
    initialHT.reset();
  }

  protected void spillPartial(Context context) throws Exception
  {
    if( spillFile == null )
    {
      spillFileHandle = context.createTempFile("jaql_group_temp", "dat");
      spillFile = new RandomAccessFile(spillFileHandle, "rw"); // TODO: use Buffered when write supported?
      spillOffsets = new LongArray();
    }
    spillOffsets.add(spillFile.getFilePointer());
    partialHT.write(spillFile, 0);
    partialHT.reset();
  }

  protected Iter aggInitial(final Context context)
  {
    return new Iter()
    {
      Iter inner = Iter.empty;
      ItemHashtable.Iterator tempIter = initialHT.iter();
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item = inner.next();
          if( item != null )
          {
            return item;
          }
          if( ! tempIter.next() )
          {
            return null;
          }
          Item key = tempIter.key();
          inner = 
            finalFn.iter(context, key,
                partialFn.iter(context, key, 
                    initialFn.iter(context, key, tempIter.values(0))));
        }
      }
    };
  }

  protected Iter aggPartial(final Context context)
  {
    return new Iter()
    {
      Iter inner = Iter.empty;
      ItemHashtable.Iterator tempIter = partialHT.iter();
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item = inner.next();
          if( item != null )
          {
            return item;
          }
          if( ! tempIter.next() )
          {
            return null;
          }
          Item key = tempIter.key();
          inner = 
            finalFn.iter(context, key,
                partialFn.iter(context, key, tempIter.values(0)));
        }
      }
    };
  }

  protected Iter aggSpill(final Context context) throws IOException
  {
    return new Iter()
    {
      Iter inner = Iter.empty;
      Merger merger = new Merger(spillFileHandle);
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item = inner.next();
          if( item != null )
          {
            return item;
          }
          
          Item key = merger.nextKey();
          if( key == null )
          {
            return null;
          }
          
          inner = finalFn.iter(context, key, merger);
        }
      }
    };
  }

  protected static class Chunk implements Comparable<Chunk>
  {
    Item key = new Item();
    long offset;
    int numValues;
    long endOffset;
    
    @Override
    public int compareTo(Chunk o)
    {
      return key.compareTo(o.key);
    }
    
  };
  
  protected class Merger extends Iter
  {
    protected PriorityQueue<Chunk> queue = new PriorityQueue<Chunk>();
    protected Item value = new Item();
    protected BufferedRandomAccessFile spillFile;
    
    public Merger(File file) throws IOException
    {
      spillFile = new BufferedRandomAccessFile(file, "r", 16*1024);
      int n = spillOffsets.size();
      spillOffsets.add(spillFile.length());
      for(int i = 0 ; i < n ; i++)
      {
        Chunk chunk = new Chunk();
        chunk.offset = spillOffsets.get(i); 
        chunk.endOffset = spillOffsets.get(i+1);
        spillFile.seek(chunk.offset);
        advanceChunk(chunk);
      }
    }
    
    /**
     * Move to the next group.  You must consume the entire group using next() before 
     * calling nextKey().  It simply skips to the next chunk of data, and does not
     * verify that the key changed.
     *  
     * @return
     * @throws Exception
     */
    public Item nextKey() throws Exception
    {
      if( queue.isEmpty() )
      {
        return null;
      }
      Chunk chunk = queue.peek();
      spillFile.seek(chunk.offset);
      return chunk.key;
    }
    

    /**
     * Returns the next value with the same key
     */
    @Override
    public Item next() throws Exception
    {
      while( true )
      {
        Item item = nextValueInChunk();
        if( item != null )
        {
          return item;
        }
        if( ! nextChunkWithSameKey() )
        {
          return null;
        }
      }
    }


    private boolean nextChunkWithSameKey() throws IOException
    {
      Chunk chunk = queue.remove();
      Chunk chunk2 = queue.peek();
      boolean same = chunk2 != null && chunk2.key.equals(chunk.key); 
      advanceChunk(chunk);
      if( same )
      {
        spillFile.seek(chunk2.offset);
      }
      return same;
    }


    private void advanceChunk(Chunk chunk) throws IOException
    {
      if( spillFile.getFilePointer() < chunk.endOffset )
      {
        chunk.key.readFields(spillFile);
        chunk.numValues = BaseUtil.readVUInt(spillFile);
        assert chunk.numValues > 0;
        chunk.offset = spillFile.getFilePointer();
        queue.add(chunk);
      }
    }

    private Item nextValueInChunk() throws IOException
    {
      Chunk chunk = queue.peek();
      if( chunk.numValues <= 0 )
      {
        return null;
      }
      value.readFields(spillFile);
      chunk.numValues--;
      return value;
    }
  }

}
