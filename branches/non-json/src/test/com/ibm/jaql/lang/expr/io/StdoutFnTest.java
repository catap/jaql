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
package com.ibm.jaql.lang.expr.io;

import org.junit.Test;

import com.ibm.jaql.AbstractTest;
import static org.junit.Assert.*;

public class StdoutFnTest extends AbstractTest {

  @Test
  public void descriptor() {
    eval("stdout();");
  }

  @Test
  public void write() {
    eval("[1,2,3]->write(stdout());");
    try {
      eval("1->write(stdout());");
      fail();
    } catch (RuntimeException e) {
      debug(e);
    }
    try {
      eval("{value: 100}->write(stdout());");
      fail();
    } catch (RuntimeException e) {
      debug(e);
    }
  }
}
