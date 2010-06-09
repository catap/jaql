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
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.util.Properties;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.ProcessRunner;

public class HashtableClient implements HashtableConstants, Closeable
{
  protected String tableUrl;
  protected DataInput in;
  protected DataOutputStream out;
  protected JsonSchema schema;
  protected BinaryFullSerializer keySerializer;
  protected BinaryFullSerializer valueSerializer;
  protected JsonValue value;
  private String host;
  private int port;

  public HashtableClient()
  {
  }
  
  protected void connect(String host, int port, int maxAttempts) throws IOException
  {
    long delay = 250;
    if( out != null )
    {
      if( host.equals(this.host) && port == this.port )
      {
        // already connected
        return;
      }
      close();
    }
    
    int i = 0;
    while( true )
    {
      try
      {
        i++;
        Socket socket = new Socket(host, port);
        this.host = host;
        this.port = port;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        return;
      }
      catch( ConnectException ex )
      {
        if( i >= maxAttempts )
        {
          throw ex;
        }
        try { Thread.sleep(delay); } catch (InterruptedException e) {}
      }
    }
  }
  
  public void startServerProcess(int port, int timeout, String memory)
  {
    Properties props = System.getProperties();
    ProcessBuilder pb = new ProcessBuilder(
        "java", // TODO: how do I find the currently running java process? props:sun.boot.library.path=C:\dev\Java\jdk1.6.0_12\jre\bin,  
        "-classpath",
        props.getProperty("java.class.path"),
        "-Xmx"+memory,
        "-Dlog4j.info",
        HashtableListener.class.getCanonicalName(),
        Integer.toString(port),
        Long.toString(timeout) );
    System.err.println("starting hashtable server");
    new ProcessRunner("hashtableServer", pb).start();
  }

  public void startServerThread(int port, int timeToLive)
  {
    Thread t = new Thread(new HashtableListener(port, timeToLive));
    t.setDaemon(true);
    t.start();
  }

  public boolean isOpen(String tableUrl)
  {
    return this.tableUrl != null && this.tableUrl.equals(tableUrl);
  }

  public boolean open(String tableUrl, int maxConnectAttempts, long age, long lease) throws IOException
  {
    this.tableUrl = null;
    URL url = new URL(tableUrl);
    connect(url.getHost(), url.getPort(), maxConnectAttempts);
    boolean buildIt = use(url.getPath(), age, lease);
    this.tableUrl = tableUrl;
    return buildIt;
  }

  protected boolean use(String tableId, long age, long lease) throws IOException
  {
    // USE tableId string, age msec, lease msec
    //   -> OK lease, schema [ Key, Value ], 
    //    | BUILD 
    //    | NOT_FOUND
    out.write( USE_CMD );
    BinaryFullSerializer.getDefault().write(out, new JsonString(tableId));
    BaseUtil.writeVSLong(out, age);
    BaseUtil.writeVSLong(out, lease);
    out.flush();
    
    byte cmd = in.readByte();
    switch( cmd )
    {
      case OK_CMD:   
        /*long leaseMS =*/ BaseUtil.readVSLong(in); // granted lease
        JsonSchema schema = (JsonSchema)BinaryFullSerializer.getDefault().read(in,null);
        setSchema( schema );
        return false;
      case BUILD_CMD:
        return true;
      case NOT_FOUND_CMD:
        throw new RuntimeException("hashtable not found: "+tableId);
      default:
        throw new ProtocolException("expected OK");
    }
  }
  
  protected void setSchema(JsonSchema schema)
  {
    this.schema = schema;
    ArraySchema aschema = (ArraySchema)schema.get();
    Schema keySchema = aschema.element(JsonLong.ZERO);
    Schema valueSchema = aschema.element(JsonLong.ONE);
    if( keySchema == null || valueSchema == null )
    {
      throw new RuntimeException("expected schema [ key, value ]");
    }
    keySerializer = new TempBinaryFullSerializer(keySchema);
    valueSerializer = new TempBinaryFullSerializer(valueSchema);
  }
  
  public void build(JsonSchema schema, JsonIterator iter) throws Exception
  {
    // SCHEMA schema [Key,Value] (PUT key, value)* OK -> OK
    setSchema(schema);
    out.write( SCHEMA_CMD );
    BinaryFullSerializer.getDefault().write(out, schema);
    JsonValue[] kv = new JsonValue[2];
    for( JsonValue i: iter )
    {
      JsonArray pair = (JsonArray)i;
      pair.getAll(kv);
      out.write( PUT_CMD );
      keySerializer.write(out, kv[0]);
      valueSerializer.write(out, kv[1]);
      // System.out.println("put: "+kv[0]+" => "+kv[1]);
    }
    out.write( OK_CMD );
    out.flush();
    
    byte cmd = in.readByte();
    if( cmd != OK_CMD )
    {
      throw new ProtocolException("expected OK");
    }
  }
  
  public JsonValue get(JsonValue key) throws IOException
  {
    if( key == null )
    {
      return null;
    }
    out.write( GET_CMD );
    keySerializer.write(out, key);
    out.flush();
    byte cmd = in.readByte();
    if( cmd == NOT_FOUND_CMD )
    {
      return null;
    }
    value = valueSerializer.read(in, value);
    return value;
  }

  // Returns [ Value ]
  // The values could be returned as a JsonIterator, but we have a problem if
  // anybody stops pulling on the iterator, then the server would be in a bad state.
  public JsonArray getBatch(final JsonArray keys) throws Exception
  {
    // GETN n, [Key]*n -> OK n [FOUND Value | NOT_FOUND]*n  OK
    if( keys == null )
    {
      return JsonArray.EMPTY;
    }
    long nn = keys.count();
    if( nn == 0 )
    {
      return JsonArray.EMPTY;
    }
    if( nn > Integer.MAX_VALUE )
    {
      throw new RuntimeException("batch too large: "+nn+" > "+Integer.MAX_VALUE);
    }
    out.write( GETN_CMD );
    int n = (int)nn;
    BaseUtil.writeVUInt(out, n);
    for( JsonValue key: keys )
    {
      keySerializer.write(out, key);
    }
    out.flush();
    byte cmd = in.readByte();
    if( cmd != OK_CMD )
    {
      throw new ProtocolException("expected OK");
    }
    int n2 = BaseUtil.readVUInt(in);
    assert n == n2;

    JsonValue val;
    SpilledJsonArray result = new SpilledJsonArray();
    for(int i = 0 ; i < n ; i++)
    {
      cmd = in.readByte();
      if( cmd == FOUND_CMD )
      {
        val = valueSerializer.read(in, null);
      }
      else if( cmd == NOT_FOUND_CMD )
      {
        val = null;
      }
      else
      {
        throw new ProtocolException("expected result value");
      }
      result.add(val);
    }
    cmd = in.readByte();
    if( cmd != OK_CMD )
    {
      throw new ProtocolException("expected OK");
    } 
    return result;
  }

  public void releaseTable() throws IOException
  {
    out.write( RELEASE_CMD );
    out.flush();
  }
  
  public void close() throws IOException
  {
    this.host = null;
    this.port = -1;
    in = null;
    if( out != null )
    {
      out.close();
      out = null;
    }
  }
}
