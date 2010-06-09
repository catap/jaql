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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.log4j.Logger;

/**
 * For enabling and disabling output to console through stdout and stderr.
 */
public class ConsolePrintEnabler {

  private static final Logger LOG = Logger.getLogger(ConsolePrintEnabler.class);

  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  private static final ByteArrayPrintStream bufOut = new ByteArrayPrintStream();
  private static final ByteArrayPrintStream bufErr = new ByteArrayPrintStream();

  /**
   * Enable or disable output to console. If console is disabled, content sent
   * to console is not printed. Instead, it is collected and will be logged when
   * console is enabled.
   * 
   * @param enable <code>true</code> to enable output to console;
   *          <code>false</code> to disable output to console.
   */
  public static void enable(boolean enable) {
    if (enable) {
      System.setOut(out);
      System.setErr(err);
      log(bufOut.getBuffer(), "stdout");
      log(bufErr.getBuffer(), "stderr");
    } else {
      bufOut.getBuffer().reset();
      bufErr.getBuffer().reset();
      System.setOut(bufOut.getPrintStream());
      System.setErr(bufErr.getPrintStream());
    }
  }

  private static void log(ByteArrayOutputStream buf, String outName) {
    String str = buf.toString();
    if (!str.equals("")) {
      LOG.info("content sent to " + outName + " when " + outName
          + " is disabled:");
      LOG.info(str);
    }
  }

  private static class ByteArrayPrintStream {
    private ByteArrayOutputStream buf;
    private PrintStream ps;

    public ByteArrayPrintStream() {
      buf = new ByteArrayOutputStream();
      ps = new PrintStream(buf);
    }

    public ByteArrayOutputStream getBuffer() {
      return buf;
    }

    public PrintStream getPrintStream() {
      return ps;
    }
  }
}
