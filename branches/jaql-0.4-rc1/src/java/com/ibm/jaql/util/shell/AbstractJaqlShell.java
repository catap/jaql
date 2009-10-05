/*
 * Copyright (C) IBM Corp. 2008.
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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;

/** Base class for version-specific shells. */
public abstract class AbstractJaqlShell {
  /** Initialize the shell using an existing cluster */
  public abstract void init() throws Exception;

  /** Initialize the shell using a mini cluster. */
  public abstract void init(String hdfsDir, int noNodes) throws Exception;

  /** Shutdown */
  public abstract void close() throws Exception;

  /**
   * @param jars
   * @throws Exception
   */
  public void addExtensions(String[] jars) throws Exception
  {
    if (jars != null) com.ibm.jaql.lang.Jaql.addExtensionJars(jars);
  }

  /**
   * @throws Exception
   */
  public void runInteractively(InputStream in) throws Exception
  {
    try
    {
      com.ibm.jaql.lang.Jaql.main(in);      
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  /**
   * @param in
   * @param failOnError
   * @param errorLog
   * @throws Exception
   */
  public void runBatch(InputStream in, boolean failOnError, PrintStream errorLog) throws Exception {
    try
    {
      if(failOnError) {
        com.ibm.jaql.lang.Jaql.batch(in);
      } else {
        com.ibm.jaql.lang.Jaql.main(in);
      } 
    } catch(Exception e) {
      // 1. write it into errorLog as JSON
      writeException(e, errorLog);
      
      // 2. flush and close the errorLog
      errorLog.flush();
      errorLog.close();
      
      // 3. wrap the exception with an exception to kill the process
      throw new Exception(e);
      
    }
  }

  private void writeException(Exception e, PrintStream errorLog) {
    
    // the message
    JString msg = new JString(e.getMessage());
    
    // the stack
    FixedJArray eArr = new FixedJArray();
    StackTraceElement[] traceElements = e.getStackTrace();
    for(int i = 0; i < traceElements.length; i++) {
      eArr.add(new JString(traceElements[i].toString()));
    }
    
    // create a record
    MemoryJRecord eRec = new MemoryJRecord();
    eRec.add("stack", eArr);
    eRec.add("msg", msg);
    
    // write out the record
    try {
      eRec.print(errorLog);
    } catch(Exception ioe) {
      ioe.printStackTrace(System.err);
    }
  }
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(AbstractJaqlShell shell, String[] args) throws Exception
  {
    //  parse arguments
    JaqlShellArguments jaqlArgs = JaqlShellArguments.parseArgs(args);

    try
    {
      if (!jaqlArgs.batchMode) {
        // TODO startup text
        System.out.println("\nInitializing Jaql.");
      }
      if (jaqlArgs.useExistingCluster) 
      {
        shell.init();
      } 
      else
      {
        shell.init(jaqlArgs.hdfsDir, jaqlArgs.numNodes);
      }
      if (jaqlArgs.jars != null) shell.addExtensions(jaqlArgs.jars);
      // TODO: runBatch if needed
      if(jaqlArgs.batchMode) {
        shell.runBatch(jaqlArgs.in, true, jaqlArgs.log);
      } else {
        shell.runInteractively(jaqlArgs.in);
      }
      if (!jaqlArgs.batchMode) {
        System.out.println("\nShutting down jaql.");
      }
      shell.close();
    }
    catch (Exception e)
    {
      if(!jaqlArgs.batchMode)
        e.printStackTrace();
      throw new Exception(e);
    }
    finally
    {
      shell.close();
    }
  }
}
