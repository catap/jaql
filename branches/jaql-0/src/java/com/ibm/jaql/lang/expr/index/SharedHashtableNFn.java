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

import java.net.ConnectException;
import java.net.URL;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.PairwiseIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.util.Bool3;


/**
 * sharedHashtableN(
 *     [Key] probeKeys,
 *     string buildUrl, // "hash://host:port/tableid",
 *     fn() returns [ [Key,Value] ] buildFn,
 *     schema [Key, Value] buildSchema ) // TODO: should be inferred from buildFn OR template params 
 *   returns [Key,Value]
 *
 * 
 * The file represented by fd must have [key,value2] pairs.
 * The [key,value2] pairs are loaded into a hash table
 * If the fd is same from call to call, the table is not reloaded.
 *   // TODO: cache multiple tables? perhaps with weak references
 *   // TODO: use hadoop's distributed cache?
 *   
 * It is generally assumed that the file is assessible wherever this
 * function is evaluated.  If it is automatically parallelized, the
 * file better be available from every node (eg, in hdfs).
 *
 * Throws an exception if the file contains duplicate keys
 * 
 * If the probe key does not exist in the hashtable, null is returned. 
 */
public class SharedHashtableNFn extends IterExpr
{
  private final static int P_DATA             =  0;
  private final static int P_URL              =  1;
  private final static int P_BUILD_FN         =  2;
  private final static int P_BUILD_SCHEMA     =  3;  
  private final static int P_AGE              =  4;
  private final static int P_LEASE            =  5;
  private final static int P_SERVER_START     =  6;
  private final static int P_SERVER_TIMEOUT   =  7;
  private final static int P_SERVER_MEMORY    =  8;
  private final static int P_SERVER_THREAD    =  9;
  
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema =
      new ArraySchema(null,
            new ArraySchema(
                new Schema[]{ 
                    SchemaFactory.anySchema(), 
                    SchemaFactory.anySchema() }));
    
    private JsonValueParameters parameters;  
    
    public Descriptor() 
    {
      Schema keyInSchema = SchemaFactory.arrayOrNullSchema();
      Schema dataSchema = SchemaFactory.nullable(new ArraySchema(null,keyInSchema));
      parameters = new JsonValueParameters(
          new JsonValueParameter[] {
              new JsonValueParameter("data", dataSchema),
              new JsonValueParameter("url", SchemaFactory.arrayOrNullSchema()),
              new JsonValueParameter("buildFn", SchemaFactory.arrayOrNullSchema()),
              new JsonValueParameter("buildSchema", SchemaFactory.arrayOrNullSchema()),
              new JsonValueParameter("age", SchemaFactory.longSchema(), JsonLong.MINUS_ONE),
              new JsonValueParameter("lease", SchemaFactory.longSchema(), JsonLong.ZERO),
              new JsonValueParameter("serverStart", SchemaFactory.booleanSchema(), JsonBool.TRUE),
              new JsonValueParameter("serverTimeout", SchemaFactory.longSchema(), new JsonLong(5*60*1000)),
              new JsonValueParameter("serverMemory", SchemaFactory.stringSchema(), new JsonString("500M")),
              new JsonValueParameter("serverThread", SchemaFactory.booleanSchema(), JsonBool.FALSE)
          });
    }

    @Override
    public Expr construct(Expr[] positionalArgs) {
      return new SharedHashtableNFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() {
      return SharedHashtableNFn.class;
    }

    @Override
    public String getName() {
      return "sharedHashtableN";
    }

    @Override
    public JsonValueParameters getParameters() {
      return parameters;
    }

    @Override
    public Schema getSchema() {
      return schema;
    }
  }

  
  protected HashtableClient htc = new HashtableClient();
  
  public SharedHashtableNFn(Expr[] exprs)
  {
    super(exprs);
  }

  /** 
   * This expression evaluates all inputs at most once
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public Schema getSchema()
  {
    // TODO: schema should come from the build fn
    Schema keySchema = exprs[P_DATA].getSchema().elements();
    if( keySchema != null && exprs[P_BUILD_SCHEMA].isCompileTimeComputable().always() )
    {
      // TODO: this should come directly from buildfn
      try
      {
        JsonSchema schema = (JsonSchema)exprs[P_BUILD_SCHEMA].compileTimeEval();
        ArraySchema aschema = (ArraySchema)schema.get();
        Schema valueSchema = aschema.elements();
        valueSchema = SchemaFactory.nullable(valueSchema);
        Schema pairSchema = new ArraySchema(new Schema[] {keySchema, valueSchema});
        return new ArraySchema(null,pairSchema);
      }
      catch( Exception e )
      {
        // TODO: report compile-time error?
      }
    }
    return SchemaFactory.arraySchema();
  }

  protected void open(Context context, String tableUrl) throws Exception
  {
    context.closeAtQueryEnd(htc); // TODO: this should only be done once...
    
    long age = ((JsonLong)exprs[P_AGE].eval(context)).get();
    long lease = ((JsonLong)exprs[P_LEASE].eval(context)).get();

    boolean buildIt;
    try
    {
      buildIt = htc.open(tableUrl, 1, age, lease);
    }
    catch( ConnectException ex )
    {
      boolean canStart = ((JsonBool)exprs[P_SERVER_START].eval(context)).get();
      if( ! canStart )
      {
        throw ex;
      }
      URL url = new URL(tableUrl);
      int port = url.getPort();
      boolean useThread = ((JsonBool)exprs[P_SERVER_THREAD].eval(context)).get();
      int timeout = ((JsonLong)exprs[P_SERVER_TIMEOUT].eval(context)).intValue();
      if( useThread )
      {
        htc.startServerThread(port, timeout);
      }
      else
      {
        String memory = ((JsonString)exprs[P_SERVER_MEMORY].eval(context)).toString();
        htc.startServerProcess(port, timeout, memory);
      }
      buildIt = htc.open(tableUrl, 10, age, lease);
    }
    
    if( buildIt )
    {
      // we need to build the table
      Function build = (Function)exprs[P_BUILD_FN].eval(context);
      if( build == null )
      {
        // we don't know how to build the table...
        htc.releaseTable();
        throw new RuntimeException("table not defined: "+tableUrl);
      }
      JsonSchema schema = (JsonSchema)exprs[P_BUILD_SCHEMA].eval(context); // TODO: this should come directly from build
      htc.build(schema, build.iter(context));
    }
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    try
    {
      JsonString jurl = (JsonString)exprs[P_URL].eval(context);
      String url = jurl.toString();
      if( ! htc.isOpen(url) )
      {
        open(context, url);
      }
      final JsonArray keys = (JsonArray)exprs[P_DATA].eval(context);
      long n = keys.count();
      if( n == 0 )
      {
        return JsonIterator.EMPTY;
      }
      // TODO: add batch size param?
      // TODO: optimize n = 1?
      //    if( n == 1 )
      //    {
      //      JsonValue val = htc.get(keys.get(0));
      //    }
      final JsonArray vals = htc.getBatch(keys);
      return new PairwiseIterator(keys.iter(), vals.iter());
    }
    catch( Exception e )
    {
      htc.close();
      throw e;
    }
  }
}
