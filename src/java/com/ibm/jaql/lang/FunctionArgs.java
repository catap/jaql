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

package com.ibm.jaql.lang;

import java.util.ArrayList;
import java.util.List;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Var;

public class FunctionArgs {
	
	protected boolean named = false;
	
	protected List<Var> params = null;
	
	public FunctionArgs(){
		params = new ArrayList<Var>();
	}
	
	  public List<Var> getParams() {
		return params;
	}

	public void setParams(List<Var> params) {
		this.params = params;
	}

	public void setArguments(Object... args){
		  //we allow to add objects
		  //but we do type check, if bad format passed to arguments, abort and throw a exception
		  // for instance , user can set (int , string , boolean ....)
		  //a loop for vars
		  for(Object arg : args){
			 if(arg instanceof String){
				 Var var = new Var("");
				 var.setValue(new JsonString((String)arg));
				 params.add(var);
			 }else if(arg instanceof Integer){
				 Var var = new Var("");
				 var.setValue(new JsonLong((Integer)arg));
				 params.add(var);
			 }else if(arg instanceof Boolean){
				 Var var = new Var("");
				 var.setValue(JsonBool.make((Boolean)arg));
				 params.add(var);
			 }else if(arg instanceof Long){
				 Var var = new Var("");
				 var.setValue(new JsonLong((Long)arg));
				 params.add(var);
			 }else if(arg instanceof Double){
				 Var var = new Var("");
				 var.setValue(new JsonDouble((Double)arg));
				 params.add(var);
			 }else if(arg instanceof Float){
				 Var var = new Var("");
				 var.setValue(new JsonDouble((Float)arg));
				 params.add(var);
			 }else if(arg instanceof JsonValue){
				 Var var = new Var("");
				 var.setValue((JsonValue)arg);
				 params.add(var);
			 }else{
				 throw new IllegalArgumentException( "Type " + arg.getClass() + " is not allowed");
			 }
		  }
		  this.named = false;
	  }
	
	
	public void setArgument(String argName, String x) {
		setArgument(argName, new JsonString(x));
	}
	
	public void setArgument(String argName, int x){
		setArgument(argName, new JsonLong(x));
	}
	
	public void setArgument(String argName, long x){
		setArgument(argName, new JsonDouble(x));
	}
	
	public void setArgument(String argName, double x){
		setArgument(argName, new JsonDouble(x));
	}
	
	public void setArgument(String argName, boolean x){
		setArgument(argName, JsonBool.make(x));
	}
	
	public void setVar(String argName, float x) {
		setArgument(argName, new JsonDouble(x));
	}

	private void setArgument(String argName, JsonValue value) {
		this.named = true;
		Var var = new Var(argName);
		var.setValue(value);
		params.add(var);
	}

}
