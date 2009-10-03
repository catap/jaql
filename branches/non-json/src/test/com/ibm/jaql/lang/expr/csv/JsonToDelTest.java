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
package com.ibm.jaql.lang.expr.csv;

import org.junit.Test;

import com.ibm.jaql.AbstractTest;

public class JsonToDelTest extends AbstractTest {

  @Test
  public void del() {
    eval("[[1,2]]->jsonToDel();");
    eval("[[1,2],[3,4]]->jsonToDel();");
    eval("[[1,2],[3,4]]->jsonToDel()->write(stdout());");
    eval("[['one','two'],['three','four']]->jsonToDel();");
    eval("[['one','two'],['three','four']]->jsonToDel()->write(stdout());");

    String records = "[{name: 'mike', age: 10}, {name: 'john', age: 20}]->jsonToDel({fields: ['name', 'age']})";
    eval(records + ";");
    eval(records + "->write(stdout());");
    
    eval("[{name: 'mike', age: 10}, [100, 200]]->jsonToDel({fields: ['name', 'age']});");
    eval("[{name: 'mike', age: 10}, [100, 200]]->jsonToDel({fields: ['name', 'age'], delimiter: '<=>'});");
    eval("[{name: 'mike', age: 10}, [100, 200]]->jsonToDel({fields: ['name', 'age'], delimiter: '<=>'})->write(stdout());");
  }
}
