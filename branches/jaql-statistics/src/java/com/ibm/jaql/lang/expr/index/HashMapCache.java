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

import java.util.HashMap;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;

//TODO: Make the server json-free?
//table id => string
//key => just hash code (so dups are ok)
//value => binary string
//TODO: Should the values live in large byte buffers?

class HashMapCache
{
  protected static final HashMapCache instance = new HashMapCache();

  public long buildTimeout = 30 * 60 * 1000; // max time to wait for a table to be built

  public static class Table
  {
    protected String tableId;
    protected int pinCount = 1; // pinned by the instantiator
    protected long buildTime = 0;
    protected long lastUsed = System.currentTimeMillis();
    protected long lastReleased = lastUsed;
    protected HashMap<JsonValue,byte[]> table = new HashMap<JsonValue, byte[]>();
    protected BinaryFullSerializer keySerializer;
    protected BinaryFullSerializer valueSerializer;
    protected JsonValue schema;

    public Table(String tableId)
    {
      this.tableId = tableId;
    }

    public boolean isBuilt()
    {
      return buildTime > 0;
    }

    public void setSchema(JsonSchema jschema)
    {
      ArraySchema aschema = (ArraySchema)jschema.get();
      Schema keySchema = aschema.element(JsonLong.ZERO);
      Schema valueSchema = aschema.element(JsonLong.ONE);
      this.schema = jschema;
      this.keySerializer = new TempBinaryFullSerializer(keySchema);
      this.valueSerializer = new TempBinaryFullSerializer(valueSchema);
    }
  }

  protected HashMap<String, Table> cache = new HashMap<String, Table>();

  // TODO: support age and lease arguments
  public Table get(String tableId, long ageMS, long leaseMS)
  {
    if( ageMS >= 0 )
    {
      throw new UnsupportedOperationException("table age is not yet implemented");
    }
    if( leaseMS > 0 )
    {
      throw new UnsupportedOperationException("table lease is not yet implemented");
    }
    Table t;
    synchronized(cache)
    {
      t = cache.get(tableId);
      if( t == null )
      {
        t = new Table(tableId);
        cache.put(tableId, t);
        return t;
      }
      t.pinCount++;
    }

    if( t.isBuilt() )
    {
      return t;
    }

    synchronized (t)
    {
      try
      {
        t.wait(buildTimeout); // could raise timeout exception
        if( ! t.isBuilt() ) // somebody failed to build the table, so we have to do it
        {
          assert t.table.isEmpty();
          return t;
        }
        t.lastUsed = System.currentTimeMillis();
        return t;
      }
      catch( InterruptedException e )
      {
        t.pinCount--;
        throw new RuntimeException("interrupted while waiting for the table "+tableId, e);
      }
    }
  }

  public void doneBuilding(Table t) 
  {
    synchronized(t)
    {
      assert t.buildTime == 0;
      t.lastUsed = t.buildTime = System.currentTimeMillis();
      t.notifyAll();
    }
  }

  public synchronized void release(Table t)
  {
    synchronized(t)
    {
      assert t.pinCount > 0;
      t.pinCount--;
      t.lastReleased = System.currentTimeMillis();
      if( ! t.isBuilt() )
      {
        // This thread was building the table, but didn't finish the job
        t.table.clear();
        if( t.pinCount == 0 )
        {
          // nobody is waiting, so remove the table from the cache
          synchronized (cache)
          {
            cache.remove(t.tableId);
          }
        }
        else
        {
          // somebody is waiting, so wake them and have them rebuild the table
          t.notify();
        }
      }
    }
  }
}
