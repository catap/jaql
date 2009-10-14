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
package com.ibm.jaql.lang.expr.system;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/**
 * A function allowing invocation of R from within Jaql.
 * 
 * R(fn, args=[item arg1, ..., item argN], 
 * inSchema=[schema arg1, ...,schema argN], outSchema=null, 
 * init, initInline=true, binary=false, flexible=false)
 * 
 * A single R process is forked per RFn instance (i.e., call site in the query).
 * The R process is forked and the init script/string is passed to R only on the 
 * first invocation.
 * 
 * To configure R, add -DR.home=<path to R> and -DR.args=<args to R> to the 
 * VM arguments. 
 * 
 * // TODO: need jaql.conf
 * 
 */
public class RFn extends Expr {
  
  // The indices to the function arguments. Too many arguments, it is better
  // to use it this way so that it is easier to reorder arguments in future.
  private static final int INDEX_FN = 0;
  private static final int INDEX_ARGS = 1;
  private static final int INDEX_IN_SCHEMA = 2;
  private static final int INDEX_OUT_SCHEMA = 3;
  private static final int INDEX_INIT = 4;
  private static final int INDEX_INIT_INLINE = 5;
  private static final int INDEX_BINARY = 6;
  private static final int INDEX_FLEXIBLE = 7;
  
  public static class Descriptor implements BuiltInFunctionDescriptor {
    private static final Class<? extends Expr> implementor = RFn.class;
    private static final String name = "R";
    private Schema schema = SchemaFactory.anySchema();
    private JsonValueParameters parameters;
    
    @SuppressWarnings("unchecked")
    public Descriptor() {
      JsonValueParameter[] params = new JsonValueParameter[8];
      params[INDEX_FN] = new JsonValueParameter("fn",SchemaFactory.stringSchema());
      params[INDEX_ARGS] = new JsonValueParameter("args",SchemaFactory.arraySchema());
      params[INDEX_IN_SCHEMA] = 
        new JsonValueParameter("inSchema",SchemaFactory.arrayOrNullSchema(),null);
      params[INDEX_OUT_SCHEMA] = new JsonValueParameter("outSchema", SchemaFactory.make(
          JsonSchema.class, null), null);
      params[INDEX_INIT] = 
        new JsonValueParameter("init", SchemaFactory.stringOrNullSchema(), null);
      params[INDEX_INIT_INLINE] = 
        new JsonValueParameter("initInline", SchemaFactory.booleanSchema(), JsonBool.TRUE);
      params[INDEX_BINARY] = 
        new JsonValueParameter("binary", SchemaFactory.booleanSchema(), JsonBool.FALSE);
      params[INDEX_FLEXIBLE] = 
        new JsonValueParameter("flexible", SchemaFactory.booleanSchema(), JsonBool.FALSE);
      parameters = new JsonValueParameters(params);
    }

    @Override
    public Expr construct(Expr[] positionalArgs) {
      return new RFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() {
      return implementor;
    }

    @Override
    public String getName() {
      return name;
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
  
  protected static String program = System.getProperty("R.home", "R");
  protected static String args = System.getProperty("R.args",
      "--no-save --no-restore --slave -q");
  protected static String cmd = program + " " + args;
  protected Process proc;
  protected BufferedReader stdout;
  protected PrintStream stdin;
  protected Throwable error;
  protected JsonParser parser;
  private Schema schema;
  private boolean flexible;
  private boolean binary;
  
  // This is the RCode which is used for the interface for transferring large data
  // into R. There is an option on putting it as an R package and installing it on
  // all the nodes, or just streaming these few lines of code into R at startup.
  // TODO: decide which approach to select.
  private static String[] initStrings = {
    "tableFromJaql = function(descriptor) { \n" +
    "  if (descriptor$mode == 2) {\n" +
    "    numfiles<-length(descriptor$name); \n" +
    "    prefix<-descriptor$path;\n" +
    "    l<- list();\n" +
    "    types<-eval(parse(text=descriptor$type));\n" +
    "    for (i in 1:numfiles) { \n" +
    "      filename<-paste(prefix,i,sep='/'); \n" +
    "      l[[descriptor$name[i]]] = scan(filename, what=eval(parse(text=types[[i]])),quiet=T); \n" +
    "      unlink(filename); \n" +
    "    }\n" +
    "    unlink(prefix,recursive=T); \n" +
    "    return(data.frame(l)); \n" +
    "  } else if (descriptor$mode == 3) {\n" +
    "    filename<-descriptor$path; \n" +
    "    type=eval(parse(text=descriptor$type));\n" +
    "    res<-scan(filename, what=type,quiet=T);\n" +
    "    unlink(filename);\n" +
    "    return(res);\n" +
    "  } else if (descriptor$mode == 4) {\n" +
    "    numfiles<-descriptor$ncols;\n" +
    "    prefix<-descriptor$path;\n" +
    "    l<- list();\n" +
    "    types<-eval(parse(text=descriptor$type));\n" +
    "    for (i in 1:numfiles) {\n" +
    "    filename<-paste(prefix,i,sep='/');\n" +
    "    l[[as.character(i)]] = scan(filename, what=eval(parse(text=types[[i]])),quiet=T);\n" +
    "    unlink(filename);\n" +
    "  }\n" +
    "  unlink(prefix,recursive=T);\n" +
    "  return(data.frame(l));\n" +
    "  } else return (NA);\n" +
    "}\n",
    "toBinary<-function(file, obj, ...) { \n" +
    "  x<-obj;\n" +
    "  save(x,file=file);\n" +
    "}"
  };
  
  static final Log LOG = LogFactory.getLog(RFn.class);

  public RFn(Expr[] exprs)
  {
    super(exprs);
    schema = SchemaFactory.anySchema();
  }

  @Override
  public JsonValue eval(Context context) throws Exception {
    try {
      if( proc == null ) {
        init(context);
      }

      JsonString fn = (JsonString)exprs[INDEX_FN].eval(context);
      if( fn == null ) {
        throw new IllegalArgumentException("R(init, fn, ...): R function required");
      }
      binary = ((JsonBool)exprs[INDEX_BINARY].eval(context)).get();
      JsonValue tmp = exprs[INDEX_OUT_SCHEMA].eval(context);
      if (tmp != null) {
        if (!(tmp instanceof JsonSchema))
          throw new IllegalArgumentException("Invalid outSchema.");
        schema = ((JsonSchema)tmp).get();
      } else if (binary) {
        schema = SchemaFactory.binarySchema();
      }
      LOG.debug("Initialized outSchema to: " + schema);
      String sep = "";
      File rOut = null;
      if (binary) {
        String tmpFileName = RUtil.getTempFileName();
        rOut = new File(tmpFileName);
        rOut.deleteOnExit();
        stdin.print("cat(toBinary(file='");
        stdin.print(tmpFileName);
        stdin.print("',");
      } else {
        stdin.print("cat(toJSON(");
      }
      stdin.print(fn);
      stdin.print("(");
      tmp = exprs[INDEX_ARGS].eval(context);
      if (tmp == null) {
        throw new IllegalArgumentException("Missing arguments to function " + fn + 
            ". For passing 0 arguments use empty array.");
      } else if (!(tmp instanceof JsonArray)) {
        throw new IllegalArgumentException("Arguments to function " + fn + 
            " must be enclosed as an array.");
      }
      JsonArray args = (JsonArray)tmp;
      tmp = exprs[INDEX_IN_SCHEMA].eval(context);
      JsonArray argSchema = null;
      if (tmp != null) {
        if (!(tmp instanceof JsonArray)) {
          throw new IllegalArgumentException("Schema for arguments of function " + fn +
              " must be enclosed in an array");
        }
        argSchema = (JsonArray)tmp;
      }
      Schema inferred = exprs[INDEX_ARGS].getSchema();
      for(int i = 0 ; i < args.count() ; i++) {
        JsonValue value = args.get(i);
        Schema elemSchema = null;
        if (argSchema != null) {
          tmp = argSchema.get(i);
          if (!(tmp instanceof JsonSchema)) {
            throw new IllegalArgumentException("Argument schema at index " + i + 
                " not an instance of " + JsonSchema.class.getCanonicalName());
          }
          elemSchema = ((JsonSchema)tmp).get();
        } else {
          elemSchema = inferred.element(new JsonLong(i));
        }
        stdin.print(sep);
        processFnArgument(context, value, elemSchema);
        sep = ",";
      }
      stdin.println(")),'\n')");
      stdin.flush();

      // parser.ReInit(stdout); 
      // TODO: we can read directly from the stdout stream, but error reporting is not so good...
      String s = stdout.readLine();
      if( s == null ) {
        throw new RuntimeException("unexpected EOF from R");
      }
      if (binary) {
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(rOut),4096);
        int length = 0;
        while ((length = in.read(buffer)) > 0) {
          out.write(buffer, 0, length);
        }
        in.close();
        rOut.delete();
        byte[] rBin = out.toByteArray();
        //System.err.println("Output: " + new String(rBin));
        return new JsonBinary(rBin);
      } else {
        parser.ReInit(new StringReader(s));
        try {
          JsonValue result = parser.JsonVal();
          return result;
        } catch( Exception e ) {
          System.err.println("Bad JSON from R:\n"+s);
          throw e;
        }
      }
    } catch (Throwable e) {
      if( error == null ) {
        error = e;
      }
      if( stdin != null ) {
        try { stdin.close(); } catch(Throwable t) {}
        stdin = null;
      } if( stdout != null ) {
        try { stdout.close(); } catch(Throwable t) {}
        stdout = null;
      }
      if( proc != null ) {
        try { proc.destroy(); } catch(Throwable t) {}
        proc = null;
      }
      if( error instanceof Exception ) {
        throw (Exception)error;
      }
      throw new UndeclaredThrowableException(error);
    }
  }
  
  private void processFnArgument(Context context, JsonValue value, 
      Schema elemSchema) throws Exception {
    flexible = ((JsonBool)exprs[INDEX_FLEXIBLE].eval(context)).get();
    if (flexible) {
      encodeAsString(value);
    } else {
      if ((value instanceof JsonAtom) || (value instanceof JsonRecord)) {
        encodeAsString(value);
      } else if (value instanceof JsonArray) {
        JsonArray array = (JsonArray)value;
        JsonRecord result = RUtil.serializeIterator(array.iter(), elemSchema, 
            new RUtil.Config());
        stdin.print("tableFromJaql(");
        encodeAsString(result, false);
        stdin.print(")");
      }
    }
  }
  
  private void encodeAsString(JsonValue value) {
    stdin.print("eval(parse(text='");
    stdin.print(RUtil.convertToRString(value));
    stdin.print("'))");
  }
  
  private void encodeAsString(JsonValue value, boolean escape) {
    stdin.print("eval(parse(text='");
    stdin.print(RUtil.convertToRString(value, escape));
    stdin.print("'))");
  }

  protected void init(Context context) throws Exception {
    LOG.info("Initializing R...");
    parser = new JsonParser();
    proc = Runtime.getRuntime().exec(cmd);    
    InputStream is = proc.getInputStream();
    stdout = new BufferedReader(new InputStreamReader(is));
    ErrorThread errorThread = new ErrorThread();
    errorThread.start();

    OutputStream os = proc.getOutputStream();
    stdin = new PrintStream(new BufferedOutputStream(os));

    JsonString initStr = (JsonString)exprs[INDEX_INIT].eval(context);
    stdin.println("sink(type='output',file=stderr())");
    // Changed it to library to avoid the message "Loading required package: rjson"
    //stdin.println("library('jaqlR')");
    for (String initString : initStrings) {
      stdin.println(initString);
    }
    stdin.println("library('rjson')");
    if( initStr != null ) {
      boolean initInline = ((JsonBool)exprs[INDEX_INIT_INLINE].eval(context)).get();
      if (initInline) {
        stdin.println(initStr);
      } else {
        throw new RuntimeException("Initialization from File not yet supported");
      }
    }
    stdin.println("sink(type='output')");
    stdin.flush();
    
    // TODO: contexts are not getting closed properly; 
    // mapreduce creates contexts, fncall creates contexts, but not cleaned up!
    context.doAtReset(
        new Runnable() {
          @Override
          public void run() {
            try {
              if( stdin != null ) {
                stdin.println("q()"); 
                stdin.close();
              }
              else if( proc != null ) {
                proc.destroy();
              }
              if( proc != null ) {
                proc.getErrorStream().close();
                int rc = proc.waitFor();
                if( rc != 0 ) {
                  LOG.error("non-zero exit code from process ["+cmd+"]: "+rc);
                }
              }
            } catch( Throwable t ) {}
          }
        }
    );
  }
  
  @Override
  public Schema getSchema() {
    return schema;
  }

  protected class ErrorThread extends Thread {
    @Override
    public void run() {
      try {
        InputStream is = proc.getErrorStream();
        byte[] buffer = new byte[1024];
        int n;
        while( (n = is.read(buffer)) >= 0 ) {
          //LOG.error(new String(buffer,0,n));
          System.err.println(new String(buffer,0,n));
        }
        is.close();
      }
      catch (Throwable e) {
        if( error == null ) {
          error = e;
        }
        proc.destroy();
      }
    }
  }
}
