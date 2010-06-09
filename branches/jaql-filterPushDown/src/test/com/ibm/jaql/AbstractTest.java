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
package com.ibm.jaql;

import java.io.StringReader;

import org.apache.log4j.Logger;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Abstract test to provide some useful methods.
 */
public class AbstractTest {

  protected Logger LOG = Logger.getLogger(getClass());

  protected void infoException(Throwable t) {
    LOG.info(null, t);
  }

  protected JsonValue parse(String str) {
    try {
      StringReader sr = new StringReader(str);
      JsonParser parser = new JsonParser(sr);
      JsonValue jv = parser.TopVal();
      return jv;
    } catch (ParseException pe) {
      throw new RuntimeException(pe);
    }
  }

  protected JsonString j(String s) {
    return s == null ? null : new JsonString(s);
  }
}
