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
    	com.ibm.jaql.lang.Jaql.main(in);      
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
