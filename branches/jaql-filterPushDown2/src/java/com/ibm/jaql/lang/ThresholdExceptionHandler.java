/*
 * Copyright (C) IBM Corp. 2010.
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

import org.apache.log4j.Logger;

public class ThresholdExceptionHandler extends ExceptionHandler {

	private static final Logger LOG = Logger.getLogger(ThresholdExceptionHandler.class.getName());
	
	private int max_exps = 0;
	private int cur_exps = 0;
	
	public ThresholdExceptionHandler() { }
	
	public ThresholdExceptionHandler(int max) {
		max_exps = max;
	}
	
	@Override
	public synchronized void handleException(Throwable error) throws Exception {
		++cur_exps;
		if( cur_exps > max_exps ) {
			throw new Exception("Number of exceptions ["+cur_exps+"] exceeded threshold [" + max_exps + "]", error);
		} else {
			LOG.warn("Processing exception ["+cur_exps+"] out of ["+max_exps+"]: ", error);
		}
	}
	
	public int getMaxExceptions() { return max_exps; }
}