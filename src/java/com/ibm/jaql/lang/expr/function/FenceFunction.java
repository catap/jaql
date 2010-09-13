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
package com.ibm.jaql.lang.expr.function;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.def.DefaultTextFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

public class FenceFunction extends IterExpr {
	
	protected BinaryFullSerializer serde = BinaryFullSerializer.getDefault();
	protected Process proc;
	private Thread inThread;
	protected Thread errThread;
	
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
	  {
	    public Descriptor()
	    {
	      super("fence", FenceFunction.class);
	    }
	  }
	
	public FenceFunction(Expr[] exprs) {
		super(exprs);
	}
	
	@Override
	public Schema getSchema() {
		return new ArraySchema(null, exprs[1].getSchema());
	}

	@Override
	public boolean isMappable(int i) {
		return i == 0;
	}
	
	@Override
	public Map<ExprProperty, Boolean> getProperties() {
		return exprs[1].getProperties();
	}
	
	@Override
	public JsonIterator iter(Context context) throws Exception {

		// the data
		JsonIterator iter = exprs[0].iter(context);
	    if( iter.isNull() )
	    {
	      return JsonIterator.NULL;
	    }
		
		// the function
		JsonValue fn = exprs[1].eval(context);
		if( fn == null ) {
			return JsonIterator.NULL;
		}
		
		// init: 		exec FenceReceiver, pass in function
		proc = (new ProcessCreator()).init((Function)fn);
		
		// main thread: consume output
		final DataInput istr = new DataInputStream(new BufferedInputStream(proc.getInputStream())); 
		
		// in thread: 	loop on input, writing to proc's stdout
		inThread = new Thread(new InputThread(iter, proc.getOutputStream()));
		inThread.start();
		
		// err thread: 	gobble proc's stderr
		errThread = new Thread(new ErrorThread(proc.getErrorStream()));
		errThread.start();
		
		return new ClosableJsonIterator() {
			
			@Override
			public boolean moveNext() throws Exception {
				
				boolean hasVal = true;
				try {
					currentValue = serde.read(istr, null); // look at reuse
				}
				catch(EOFException eof) {
					// is there a better way to detect EOS? JsonInputStream does this via EOFException
					hasVal = false;
					currentValue = null;	
					int rc = proc.waitFor();
					if( rc != 0 ) {
						throw new RuntimeException("child process exited with non-zero return code");
					}
				}
				catch(Exception e) {
					proc.destroy();
					throw new RuntimeException(e);
				}
				
				return hasVal;
			}
			
			@Override
			public void close() throws IOException {
				proc.destroy();
				try {
					if( inThread.isAlive() ) {
						inThread.join(1000);
						inThread.interrupt();
					}
					if( errThread.isAlive() ) {
						errThread.join(1000);
						errThread.interrupt();
					}
				} catch(InterruptedException ie) {
					throw new RuntimeException(ie);
				}
			}
		};
	}
	
	static class ProcessCreator {
		public Process init(Function fn) throws Exception {
			
			String jvmCmd = new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath();
			
			String cpName = "-classpath";
			String cpVal  = System.getProperty("java.class.path");
			File newJar = ClassLoaderMgr.getExtensionJar();
			if( newJar != null ) {
				cpVal += File.pathSeparatorChar + newJar.getAbsolutePath();
			}
			
			//String ldirName = "-Dhadoop.log.dir=";
			//String ldirVal  = new File(System.getProperty("hadoop.log.dir")).getAbsolutePath();
			
			String llevName = "-Dhadoop.root.logger=";
			String llevVal  = "INFO,console";
			
			String evaluator = FenceReceiver.class.getName();
			
			JsonRecord args = createArgs(fn);
			String argsStr = JsonUtil.printToString(args);
			argsStr = argsStr.replaceAll("\"", "\\\\\""); // want " -> \" to quote on command-line
			
			//BaseUtil.LOG.info(jvmCmd +" | " + llevName+llevVal + " | " + cpName + " | " + cpVal + " | " + evaluator + " | " + argsStr);
			
			ProcessBuilder pb = new ProcessBuilder(jvmCmd, /**ldirName+ldirVal,*/ llevName+llevVal, cpName, cpVal, evaluator, argsStr);
			
			return pb.start();
		}
		
		public JsonRecord createArgs(Function fn) {
			BufferedJsonRecord args = new BufferedJsonRecord();
			args.add(new JsonString("func"), fn);
			
			return args;
		}
	}
	
	class InputThread implements Runnable {

		JsonIterator iter;
		DataOutputStream ostr;
		
		
		public InputThread(JsonIterator iter, OutputStream ostr) {
			this.iter = iter;
			this.ostr = new DataOutputStream(ostr);
		}
		
		@Override
		public void run() {
			Throwable error = null;
			try {
				for(JsonValue v : iter) {
					serde.write(ostr, v);
				}
			} catch(Exception e) {
				error = e;
				throw new RuntimeException(e);
			} finally {
				if( error == null ) {
					try {
						ostr.flush();
						ostr.close();
					} catch(Exception ee) {
						throw new RuntimeException(ee);
					}
				} else {
					proc.destroy();
					throw new UndeclaredThrowableException(error);
				}
			}
		}		
	}
	
	class ErrorThread implements Runnable {
		
		InputStream istr;
		
		public ErrorThread(InputStream istr) {
			this.istr = istr;
		}
		
		@Override
		public void run() {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(istr));
			String line = null;
			try {
				while( (line = rdr.readLine()) != null ) {
					System.err.println(line);
				}
			} catch(Exception e) {
				proc.destroy();
				throw new RuntimeException(e);
			}
		}
	}
}