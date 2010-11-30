/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.lang.expr.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.ClassLoaderMgr;

// TODO: add an async option and return a handle. add an additional expression to manage an MR handle.
//       it will be able to do things like kill the job, report on its status, etc.
/**
 * Usage: { status: boolean } nativeMR( { job conf settings } conf , { apiVersion: "0.0" | "1.0" } options );
 * 
 * Launch a stand-alone map-reduce job that is exclusively described by job conf settings.
 * 
 * Example: { status: true } nativeMR( loadJobConf( "myJob.conf" ) );
 */
public class NativeMapReduceExpr extends Expr
{
  protected static final Logger LOG = Logger.getLogger(NativeMapReduceExpr.class.getName());
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
  {
    public Descriptor()
    {
      super("nativeMR", NativeMapReduceExpr.class);
    }
  }
 
  public static final JsonString STATUS = new JsonString("status");
  private static final Map<ExprProperty, Boolean> properties = new EnumMap<ExprProperty, Boolean>(ExprProperty.class);
  static {
    properties.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, false);
    properties.put(ExprProperty.HAS_CAPTURES, true);
    properties.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    properties.put(ExprProperty.IS_NONDETERMINISTIC, true);
    properties.put(ExprProperty.READS_EXTERNAL_DATA, true);
  };
  private static final String VERSION_0_0 = "0.0";
  private static final String VERSION_1_0 = "1.0";
  private static final JsonString VERSION_NAME = new JsonString("apiVersion");
  private String apiVersion = VERSION_0_0;
  
  private static final JsonString USE_SESSION_JAR = new JsonString("useSessionJar");
  private JsonBool useSessionJarDefault = JsonBool.FALSE;
  
  /**
   * @param exprs
   */
  public NativeMapReduceExpr(Expr[] exprs)
  {
    super(exprs);
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#getProperties()
   */
  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    return properties;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#getSchema()
   */
  @Override
  public Schema getSchema() 
  {
    return SchemaFactory.recordSchema();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    // get the conf values from the parameter
    JsonRecord confRec = (JsonRecord)exprs[0].eval(context);
    
    // get the options (if they exist) and process them
    JsonRecord optsRec = (JsonRecord)exprs[1].eval(context);
    if( optsRec != null ) {
      // option that specifies hadoop API version
      JsonValue v = optsRec.get(VERSION_NAME);
      if( v != null && v.getType() == JsonType.STRING ) {
        String vs = ((JsonString)v).toString();
        if( vs.equals(VERSION_1_0)) {
          apiVersion = vs;
        }
      }
      // option that specifies whether jaql's session jar is to be used
      v = optsRec.get(USE_SESSION_JAR);
      if( v != null && v.getType() == JsonType.BOOLEAN ) {
    	  useSessionJarDefault = (JsonBool) v;
      }
    }
    
    // set up the conf
    Configuration conf = new Configuration();
    for(Map.Entry<JsonString, JsonValue> e : confRec) {
      String k = e.getKey().toString();
      JsonValue val = e.getValue();
      if( !(val instanceof JsonString)) {
        throw new Exception("conf value must be of type String: " + val.getType() + "," + val);
      }
      String v = ((JsonString)val).toString();
      conf.set(k, v);
    }
    
    if( apiVersion.equals(VERSION_0_0)) {
      return eval_0_0(conf);
    } else {
      return eval_1_0(conf);
    }
  }
  
  private JsonRecord eval_0_0(Configuration conf) throws Exception {
    
    JobConf job = new JobConf(conf);
    // set the jar if needed
    if( useSessionJarDefault.get() ) {
    	File jFile = ClassLoaderMgr.getExtensionJar();
    	if( jFile != null ) {
    		job.setJar(jFile.getAbsolutePath());
    	} else {
    		job.setJarByClass(NativeMapReduceExpr.class);
    	}
    }
    
    // submit the job
    boolean status = true;
    try {
      //JobClient.runJob(job);
    	Util.submitJob(new JsonString(NativeMapReduceExpr.class.getName()), job);
    } catch(IOException e) {
      status = false;
      e.printStackTrace();
      LOG.warn("native map-reduce job failed", e);
    }
    // setup the return value
    BufferedJsonRecord ret = new BufferedJsonRecord();
    ret.add(STATUS, (status) ? JsonBool.TRUE : JsonBool.FALSE );
    
    return ret;
  }
  
  private JsonRecord eval_1_0(Configuration conf) throws Exception {
    
    boolean status = true;
    
    Job job = null;
    // set the jar if needed
    if( useSessionJarDefault.get() ) {
    	File jFile = ClassLoaderMgr.getExtensionJar();
    	conf.set("mapred.jar", jFile.getAbsolutePath());
    	job = new Job(conf);
    } else {
    	job = new Job(conf);
    	job.setJarByClass(NativeMapReduceExpr.class);
    }
    
    try {
      job.waitForCompletion(true);
    } catch(Exception e) {
      status = false;
      e.printStackTrace();
      LOG.warn("native map-reduce job failed", e);
    }
    // setup the return value
    BufferedJsonRecord ret = new BufferedJsonRecord();
    ret.add(STATUS, (status) ? JsonBool.TRUE : JsonBool.FALSE );
    
    return ret;
  }
}