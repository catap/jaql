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
package com.ibm.jaql.lang;

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;

/**
 * A printer using a print stream. It is used in the following situations:
 * <ul>
 * <li>JAQL shell interactive mode</li>
 * <li>No output options are provided in JAQL shell batch mode</li>
 * </ul>
 */
public class StreamPrinter implements JaqlPrinter {

  private PrintStream output;
  protected String prompt = "\njaql> ";
  private boolean batchMode;
  private boolean printTime = 
    System.getProperty("jaql.time.results", "false").toLowerCase().equals("true");

  public StreamPrinter(PrintStream output, boolean batchMode) {
    this.output = output;
    this.batchMode = batchMode;
  }

  @Override
  public void print(Expr expr, Context context) throws Exception {
    Schema schema = expr.getSchema();
    long time = System.currentTimeMillis();;
    if (expr.getSchema().is(ARRAY, NULL).always()) {
      JsonIterator iter = expr.iter(context);
      Schema elementSchema = schema.elements();
      if (elementSchema != null) {
        iter.print(output, 0, elementSchema);
      }
      else
      {
        // empty array or null
        iter.print(output, 0);
      }
      
    } else {
      JsonValue value = expr.eval(context);
      // special casing of explains: result should not be converted
      if (expr instanceof ExplainExpr) {
        output.print(value);
      } else {
        JsonUtil.getDefaultSerializer(schema).write(output, value);
      }
    }
    output.println();
    if( printTime )
    {
      time = System.currentTimeMillis() - time;
      System.err.println("   time: "+time+" ms");
    }
    output.flush();
  }

  /**
   * Set the prompt displayed before each statement. Set to "" to disable.
   * 
   * @param prompt
   */
  public void setPrompt(String prompt) {
    this.prompt = prompt;
  }

  public void setOutput(PrintStream output) {
    this.output = output;
  }

  public void printPrompt() throws IOException {
    if (!batchMode)
      output.print(prompt);
  }

  @Override
  public void close() throws IOException {};
}