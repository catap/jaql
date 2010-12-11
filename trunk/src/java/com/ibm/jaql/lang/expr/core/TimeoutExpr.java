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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Wrap any expression to limit the amount of time it will run.
 * Usage:
 * 
 * T timeout(T e, long millis);
 *
 * Given an arbitrary expression e (of type T), all it to be evaluated 
 * for now more than millis ms. If e completes in less than millis time,
 * then its value is returned. Otherwise, an exception is thrown.
 * 
 * @jaqlExample sleep = javaudf("com.ibm.jaql.fail.fn.SleepFn"); // simple function where we can control its evaluation time
 * 
 * @jaqlExample timeout(sleep(10000), 5000); // this should throw an exception
 * 
 * @jaqlExample timeout(sleep(5000), 10000)); // this should complete successfully in 5 seconds
 */
public class TimeoutExpr extends Expr {

  public static long DEFAULT_TIMEOUT = 10000;
  
  public static enum TaskState { UNKNOWN, OK, TIMEOUT, ERROR };
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
  {
    public Descriptor()
    {
      super("timeout", TimeoutExpr.class);
    }
  }
  
  public TimeoutExpr(Expr[] exprs) {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception {
    
    Expr expr = exprs[0];
    long timeOut = DEFAULT_TIMEOUT;
    if(exprs[1] != null) {
      JsonValue v = exprs[1].eval(context);
      if( v instanceof JsonNumber ) {
	timeOut = ((JsonNumber)v).longValue();
      } else {
	throw new Exception("illegal timeout specified");
      }
    }
    
    // 1. create an evaluator thread t
    ExprEvaluator evaluator = new ExprEvaluator(expr, context);
    Thread t = new Thread(evaluator);
    t.start();
    try {
      // 2. let the spawned thread go for no more than timeOut
      t.join(timeOut);
    } catch(InterruptedException e) {
      throw new RuntimeException(e);
    }
    // 3. if t is not done, kill it
    if(t.isAlive()) {
	t.interrupt();
	throw new Exception("task took longer than: " + timeOut + " millis");
    }
    
    // 4. t completed so get its value
    if(evaluator.getState() == TaskState.OK) {
      return evaluator.getValue();
    }
    
    if(evaluator.getState() == TaskState.TIMEOUT || evaluator.getState() == TaskState.ERROR) {
      throw new Exception(evaluator.getException());
    }
     
    throw new RuntimeException("Task in unknown state");
  }

  class ExprEvaluator implements Runnable {

    Expr expr;
    Context ctx;
    JsonValue val = null;
    TaskState state = TaskState.UNKNOWN; // -1: don't know, 0: ok, 1: interrupted, 2: encoutered exception
    Exception exp = null;
    
    ExprEvaluator(Expr e, Context c) {
      expr = e;
      ctx = c;
    }
    
    @Override
    public void run() {
      try {
	val = expr.eval(ctx);
      } catch(InterruptedException ie) {
	// this task took long and we were interrupted
	exp = ie;
	state = TaskState.TIMEOUT;
	return;
      } catch(Exception e) {
	// some exception was thrown when evaluating the expr
	state = TaskState.ERROR;
	exp = e;
	return;
      }
      state = TaskState.OK;
    }
    
    TaskState getState() {
      return state;
    }
    
    Exception getException() {
      return exp;
    }
    
    JsonValue getValue() {
      return val;
    }
  }
  
  @Override
  public Schema getSchema() {
    return exprs[0].getSchema();
  }
}