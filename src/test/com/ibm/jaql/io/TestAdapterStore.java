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
package com.ibm.jaql.io;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.ibm.jaql.AbstractTest;
import com.ibm.jaql.json.type.JsonString;

public class TestAdapterStore extends AbstractTest {

  @Test
  public void adapterStore() throws Exception {
    getRegistry("jsonStream");
    getRegistry("delStream");
    getRegistry("xmlStream");
  }

  private AdapterStore.AdapterRegistry getRegistry(String type) {
    AdapterStore as = AdapterStore.getStore();
    assertNotNull(as);
    JsonString key = new JsonString(type);
    AdapterStore.AdapterRegistry ar = as.get(key);
    debug("outoptions: " + ar.getOutput());
    return ar;
  }

}