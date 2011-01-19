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
package com.ibm.jaql.fail.fn;

import java.io.IOException;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;

public class ErrorFn {
	
	public static enum Error { NONE, SHALLOW_ERROR, DEEP_ERROR };
	
	public JsonString eval(JsonString sVal, JsonLong lVal, JsonString eType) throws Exception {
		
		Error e = Error.valueOf(eType.toString());
		if(e.equals(Error.SHALLOW_ERROR)) {
			throw new Exception("shallow");
		} else if(e.equals(Error.DEEP_ERROR)) {
			try {
				deepError();
			} catch(Throwable t) {
				throw new Exception(t);
			}
		}
		// else NO_ERROR
		
		return sVal;
	}
	
	private void deepError() throws IOException {
		throw new IOException("caused by IO");
	}
}