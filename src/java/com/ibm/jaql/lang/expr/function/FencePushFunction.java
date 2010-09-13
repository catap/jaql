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
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

public class FencePushFunction extends Expr {
	
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
	  {
	    public Descriptor()
	    {
	      super("fencePush", FencePushFunction.class);
	    }
	  }
	
	private BinaryFullSerializer serde = BinaryFullSerializer.getDefault();
	private Process proc;
	private Thread errThread;
	private DataInputStream istr;
	private DataOutputStream ostr;
	private boolean init = false;
	
	public FencePushFunction(Expr[] exprs) {
		super(exprs);
	}

	@Override
	public Schema getSchema() {
		return exprs[1].getSchema();
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
	public JsonValue eval(Context context) throws Exception {
		// the data
		final JsonValue val = exprs[0].eval(context);
	    if( val == null )
	    {
	      return null;
	    }
		
	    if( !init ) {
	    	
	    	init = true;
	    	
	    	// the function
	    	JsonValue fn = exprs[1].eval(context);
	    	if( fn == null ) {
	    		return null;
	    	}

	    	// init: 		exec FenceReceiver, pass in function
	    	
	    	proc = (new ProcessCreator()).init((Function)fn);

	    	// main thread: consume output
	    	istr = new DataInputStream(new BufferedInputStream(proc.getInputStream())); 
	    	ostr = new DataOutputStream(new BufferedOutputStream(proc.getOutputStream()));

	    	// err thread: 	gobble proc's stderr
	    	errThread = new Thread(  (new FenceFunction(Expr.NO_EXPRS)).new ErrorThread(proc.getErrorStream()));
	    	errThread.start();
	    	
	    	// register clean-up thread in Context to close off FenceReceiver in push mode
	    	context.doAtReset(new Runnable() {
	    		@Override
	    		public void run() {
	    			proc.destroy();
					try {
						ostr.flush();
						ostr.close();
						if( errThread.isAlive() ) {
							errThread.join(1000);
							errThread.interrupt();
						}
					} 
					catch(InterruptedException ie) {
						throw new RuntimeException(ie);
					}
					catch(IOException ioe) {
						throw new RuntimeException(ioe);
					}
	    		}
	    	});
	    }
	    
	    if( proc == null ) {
	    	return null;
	    }
		// TODO: for push mode, this becomes synchronous. just write/flush to stdout and read from stdin
		// TODO: consider a flush mode in FenceReceiver for push mode
	
	    JsonValue rVal = null;
	    // write to proc
	    try {
	    	serde.write(ostr, val);
	    	ostr.flush();

	    	rVal = serde.read(istr, null); // look at reuse
	    }
	    catch(EOFException eof) {
	    	// is there a better way to detect EOS? JsonInputStream does this via EOFException	
	    	int rc = proc.waitFor();
	    	if( rc != 0 ) {
	    		throw new RuntimeException("child process exited with non-zero return code");
	    	}
	    }
	    catch(Exception e) {
	    	proc.destroy();
	    	throw new RuntimeException(e);
	    }

	    return rVal;
	}
	
	class ProcessCreator extends FenceFunction.ProcessCreator {
		public ProcessCreator() { super(); }

		@Override
		public JsonRecord createArgs(Function fn) {
			BufferedJsonRecord args = (BufferedJsonRecord)super.createArgs(fn);
			args.add(new JsonString("sync"), JsonBool.TRUE);
			
			return args;
		}
		
		
	}
}