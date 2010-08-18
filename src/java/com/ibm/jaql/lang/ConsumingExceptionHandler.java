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

import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.type.JsonValue;

/** Consume the rest of the parser input and rethrow the exception. */
public class ConsumingExceptionHandler extends ExceptionHandler
{
  private Jaql jaql;

  public ConsumingExceptionHandler(Jaql jaql)
  {
    this.jaql = jaql;
  }
  
  @Override
  public void handleException(Throwable ex, JsonValue ctx) throws Exception
  {
    ex.printStackTrace(); // TODO: LOG
    jaql.setInput(null);
    if( ex instanceof Exception )
    {
      throw (Exception)ex;
    }
    throw new UndeclaredThrowableException(ex);
  }
}
