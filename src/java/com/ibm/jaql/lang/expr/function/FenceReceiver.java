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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.log4j.Logger;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/**
 * Reads a value from stdin, binds it to F, eval's F, writes value back to stdout
 *
 */
public class FenceReceiver {

	private static final Logger LOG = Logger.getLogger(FenceReceiver.class.getName());
	
	private BinaryFullSerializer serde = BinaryFullSerializer.getDefault();
	private boolean sync = false;
	private Function fn;
	private DataInput in;
	private DataOutputStream out;
	private PrintStream err;
	private Context ctx = new Context();
	// TODO: log4j output is being swallowed-- figure out a way to log appropriately
	
	/**
	 * @param args
	 * @param in
	 * @param out
	 * @param err
	 * @throws Exception
	 */
	public FenceReceiver(String args, InputStream in, OutputStream out, OutputStream err) throws Exception {

		// Expect a single argument that encodes a JsonRecord
		JsonRecord argsRec = null;
		LOG.info("jaql child process args: " + args);
		try {
			JaqlLexer lexer = new JaqlLexer( new StringReader(args));
			JaqlParser parser = new JaqlParser(lexer);
			Expr e = parser.parse();
			argsRec = (JsonRecord) e.eval(ctx);
		} catch(Exception e) {
			throw new UndeclaredThrowableException(e, "Error setting up jaql child process function");
		} 

		// get the function from args
		fn = (Function) argsRec.get(new JsonString("func"));
		
		// process other args
		JsonValue val = argsRec.get(new JsonString("sync")); // TODO: make static key
		if(val != null) {
			sync = ((JsonBool)val).get();
		}
		
		// setup the inputs/outputs
		this.in = new DataInputStream(in);
		this.out = new DataOutputStream(new BufferedOutputStream(System.out));
		this.err = new PrintStream(err);
	}

	/**
	 * 
	 */
	public void run() throws Exception {

		try {
			// loop until EOF
			while( true ) {

				// read from in a JsonValue
				JsonValue input = serde.read(in, null);					

				// bind it into the function
				fn.setArguments(input);

				// evaluate the function
				JsonValue output = fn.eval(ctx);

				// write to out the value
				serde.write(out, output);
				if( sync )
					out.flush();
			}	
		} 
		catch(EOFException eof) {
			LOG.info("jaql child process read EOF");			
		}
		catch(Throwable t) {
			throw new UndeclaredThrowableException(t, "Error processing data in jaql child process");
		}
		finally {
			out.write(-1);
			out.flush();
			err.flush();
		}
	}	
	
	// 
	// args[0]: the record containing the function
	// args[1]: open up a back-door to read from a file so that we can test independently. 
	//          format must be just a binary stream of json values 
	//
	public static void main(String[] args)  { 
		
		int rc = 0;
		
		// setup source of input data
		InputStream input = System.in;
		if( args.length == 2 ) {
			try {
				input = new BufferedInputStream(new FileInputStream(new File(args[1])));
			} catch(Throwable t) {
				LOG.error("Unable to open file for testing jaql child process", t);
				rc = 1;
			}
		}
		
		// process input data
		if( rc == 0 ) {
			try {
				FenceReceiver f = new FenceReceiver(args[0], input, System.out, System.err);
				f.run();
			} catch(Throwable t) {
				LOG.error("Error processing function in jaql child process", t);
				rc = 1;
			}
		}
		
		System.exit(rc);
	}
}