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
      com.ibm.jaql.lang.Jaql.run("<unknown>", in);   // TODO: get filename   
    }
    catch (Exception e)
    {
      e.printStackTrace();
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
      shell.runInteractively(jaqlArgs.in);
      if (!jaqlArgs.batchMode) {
        System.out.println("\nShutting down jaql.");
      }
      shell.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      shell.close();
    }
  }
}
