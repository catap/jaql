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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.Socket;

import org.apache.hadoop.io.DataOutputBuffer;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;


public class HashtableServer implements HashtableConstants, Runnable
{
  protected Socket socket;
  protected DataInputStream in;
  protected DataOutputStream out;
  protected HashMapCache.Table table = null;
  protected static final BinaryFullSerializer defaultSerializer = BinaryFullSerializer.getDefault();

  public HashtableServer(Socket socket) throws IOException
  {
    this.socket = socket;
    in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
  }
  
  @Override
  public void run()
  {
    JsonValue readKey = null;
    JsonValue[] keys = new JsonValue[0];
    
    try
    {
      while( true )
      {
        byte command = in.readByte();
        switch( command )
        {
          // GET Key -> FOUND Value | NOT_FOUND
          case GET_CMD:
          {
            readKey = table.keySerializer.read(in, readKey);
            byte[] value = table.table.get(readKey);
            if( value == null )
            {
              out.write( NOT_FOUND_CMD );
            }
            else
            {
              out.write( FOUND_CMD );
              out.write(value);
            }
            break;
          }
          // GETN n, [Key]*n -> OK n [FOUND Value | NOT_FOUND]*n  OK
          case GETN_CMD:
          {
            int n = BaseUtil.readVUInt(in);
            if( n > keys.length ||    // bigger array required
                3 * n < keys.length ) // array is way too big
            {
              keys = new JsonValue[n];
            }
            for(int i = 0 ; i < n ; i++)
            {
              keys[i] = table.keySerializer.read(in, keys[i]);
            }
            out.write( OK_CMD );
            BaseUtil.writeVUInt(out,n);
            for(int i = 0 ; i < n ; i++)
            {
              byte[] value = table.table.get(keys[i]);
              if( value == null )
              {
                out.write( NOT_FOUND_CMD );
              }
              else
              {
                out.write( FOUND_CMD );
                out.write(value);
              }
            }
            out.write( OK_CMD );
            break;
          }
          // USE tableId string, age msec, lease msec
          //   -> OK lease, schema [ Key, Value ], 
          //    | BUILD 
          case USE_CMD:
          {
            if( table != null )
            {
              HashMapCache.instance.release(table);
              table = null;
            }
            JsonString tableId = (JsonString)defaultSerializer.read(in, null);
            long ageMS = BaseUtil.readVSLong(in);
            long leaseMS = BaseUtil.readVSLong(in);
            
            table = HashMapCache.instance.get(tableId.toString(), ageMS, leaseMS);
            if( table.isBuilt() ) // The table is good to go
            {
              out.write( OK_CMD );
              BaseUtil.writeVSLong(out, 0); // TODO: implement leases
              defaultSerializer.write(out, table.schema);
            }
            else // We need to build the table
            {
              out.write( BUILD_CMD );
              out.flush();
              
              // SCHEMA schema [Key,Value] (PUT key, value)* OK -> OK
              command = in.readByte();
              if( command == RELEASE_CMD )
              {
                // The client couldn't build the table, so just release it
                HashMapCache.instance.release(table);
                break;
              }
              if( command != SCHEMA_CMD )
              {
                throw new ProtocolException("expected SCHEMA");  
              }
              table.setSchema( (JsonSchema)defaultSerializer.read(in, null) );
              DataOutputBuffer buf = new DataOutputBuffer();
              
              System.err.println("building hashtable "+table.tableId);

              while( (command = in.readByte()) == PUT_CMD )
              {
                // TODO: we need to use a spilling hashtable to avoid memory overflows...
                // TODO: we could at least pack the values more tightly 
                buf.reset();
                JsonValue key = table.keySerializer.read(in, null); // Be sure NOT to reuse the key here!
                table.valueSerializer.copy(in, buf);
                byte[] val = new byte[buf.getLength()];
                System.arraycopy(buf.getData(), 0, val, 0, val.length);
                table.table.put(key, val);
              }
              if( command != OK_CMD )
              {
                throw new ProtocolException("expected OK");  
              }
              HashMapCache.instance.doneBuilding(table);
              out.write( OK_CMD );
              System.err.println("built hashtable "+table.tableId);
            }
            break;
          }
          // RELEASE -> OK
          case RELEASE_CMD:
          {
            if( table != null )
            {
              HashMapCache.instance.release(table);
              table = null;
            }
            out.write( OK_CMD );
            break;
          } 
          // LIST_TABLES -> (FOUND tableId built age lease schema numEntries)* OK
          // GET_ALL -> (FOUND key value)* OK
          // UNDEFINE tableId -> OK | NOT_FOUND
          // UNDEFINE_ALL -> OK
          default:
            throw new ProtocolException("invalid command code");  
        }
        out.flush();
      }
    }
    catch( EOFException e )
    {
      // ignored
    }
    catch( Exception e )
    {
      // log and exit thread
      e.printStackTrace();
    }
    finally
    {
      if( table != null )
      {
        HashMapCache.instance.release(table);
      }
      try
      {
        socket.close();
      }
      catch( Exception e )
      {
        // log and exit thread
        e.printStackTrace();
      }
    }
  }
}
