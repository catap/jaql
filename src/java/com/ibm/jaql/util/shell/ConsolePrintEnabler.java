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
package com.ibm.jaql.util.shell;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * For enabling and disabling ouput to STDOUT and STDERR. STDERR is disabled
 * since <code>MiniDFSCluster</code> prints startup information to it.
 */
public class ConsolePrintEnabler {

  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  private static final PrintStream dummy = new PrintStream(new OutputStream() {
    @Override
    public void write(int n) {}
  });

  /**
   * Enable or disable output to STDOUT. If STDOUT is disabled,
   * <code>write</code> method of {@link System#out} does nothing.
   * 
   * @param enable <code>true</code> to enable output to STDOUT;
   *          <code>false</code> to disable output to STDOUT.
   */
  public static void enable(boolean enable) {
    if (enable) {
      System.setOut(out);
      System.setErr(err);
    } else {
      System.setOut(dummy);
      System.setErr(dummy);
    }
  }
}
