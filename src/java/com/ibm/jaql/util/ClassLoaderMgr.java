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
package com.ibm.jaql.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * import com.ibm.jaql.lang.expr.index.ProbeLuceneFn;
 */
public class ClassLoaderMgr
{
	
  private static ClassLoader classLoader     = null;
  private static JarCreator creator;
  
  static { reset(); }
  
  /** Reset the class loader manager - use with caution */
  public static void reset()
  {
    creator = new JarCreator();
    creator.setDaemon(true);
    creator.start();
  }
  
  /**
   * This function should only be used during startup, because it does not use
   * the searchpath (yet?).
   * @param names
   * @return
   * @throws Exception
   */
  public static void addExtensionJars(String[] names) throws Exception
  {
		if (names == null || names.length == 0)
			return;

		int len = names.length;
		File[] files = new File[len];
		for(int i = 0; i < len; i++) {
			files[i] = new File(names[i]);
		}
		addExtensionJars(files);		
  }
  
  
  public static void addExtensionJars(File[] files) throws Exception
  {
	  if (files == null || files.length == 0)
			return;
	  
	  // create a collection of jar files with valid paths
	  ArrayList<File> jars = new ArrayList<File>();
	  for(int i = 0; i < files.length; i++) {
		  File jar = files[i];
		  if(!jar.exists()) {
			  BaseUtil.LOG.info("specified jar path invalid: " + jar);
		  } else {
			  jars.add(jar);
		  }
	  }
		
	  // add all jars to same classloader
	  classLoader = createClassLoader(jars);

	  // set the current thread's classloader
	  Thread.currentThread().setContextClassLoader(classLoader);

	  // add each jar to the todo list
	  for (File jar : jars) {
		  //Lazy complete jar creation
		  creator.addExtensionJar(jar);

		  BaseUtil.LOG.info("jars added to classloader: " + jar);
	  }
  }
  /**
   * Combines the extension jars and the original jar into one jar. When several
   * jars contain the same file/class the first version is used. The order in
   * which the jars are inserted into the new jar is first jaql, then the
   * extensions in the order in which they were defined.
   * 
   * @param extensions paths of the extension jars
   */
//  public static void addExtensionJar(File jar) throws Exception {
//	  if(!jar.exists())
//		  return;
//
//	  // add the new jar to the classloader
//	  classLoader = createClassLoader(jar);
//	  // set the current thread's classloader
//	  Thread.currentThread().setContextClassLoader(classLoader);
//
//	  //Lazy complete jar creation
//	  creator.addExtensionJar(jar);
//
//	  return;
//  }

  /**
   * @return
   * @throws IOException 
   */
  public static File getExtensionJar() throws IOException
  {
  	return creator.getExtensionJar();
  }

  /**
   * @param cName
   * @return
   */
  public static Class<?> resolveClass(String cName)
  {
    // use the current classloader and register it
    Class<?> c = null;
    try
    {
      if (classLoader == null)
        c = Class.forName(cName);
      else
        c = Class.forName(cName, true, classLoader);
    }
    catch (ClassNotFoundException e)
    {
      throw new RuntimeException(e);
    }
    return c;
  }
  
  /**
   * 
   * @param name
   * @return
   */
  public static URL getResource(String name)
  {
    ClassLoader cl = getClassLoader();
    URL loc = cl.getResource(name);
    if( loc == null )
    {
      cl = ClassLoader.getSystemClassLoader();
      loc = cl.getResource(name);
    }
    return loc;
  }
  
	public static InputStream getResourceAsStream(String name) throws IOException 
	{
		return getResource(name).openStream();
	}

  public static ClassLoader getClassLoader()
  {
    ClassLoader cl = classLoader;
    if( cl == null )
    {
      cl = ClassLoaderMgr.class.getClassLoader();
    }
    return cl;
  }
  
  /**
   * @param paths
   * @return
   * @throws Exception
   */
  private static URLClassLoader createClassLoader(File jar) throws Exception
  {
  	//There are no file checks done at this point it has to be valid
    URL[] urls = {jar.toURI().toURL()};
    ClassLoader parent = (classLoader == null)
        ? JsonValue.class.getClassLoader()
        : classLoader;
    return new URLClassLoader(urls, parent);
  }
  
  private static URLClassLoader createClassLoader(List<File> jars) throws Exception
  {
	  int numPaths = jars.size();
	  if (numPaths == 0) return null;

	  URL[] urls = new URL[numPaths];
	  for (int i = 0; i < numPaths; i++)
	  {
		  File jar = jars.get(i);
		  urls[i] = jar.toURI().toURL();
	  }
	  ClassLoader parent = (classLoader == null)
	  ? JsonValue.class.getClassLoader()
			  : classLoader;
	  return new URLClassLoader(urls, parent);  
  }
}

/**
 * Delayed jar loading is not possible because then metadata cannot be read in time. It IS possible
 * because metadata can be loaded with getResource("...") Statement
 *
 */
final class JarCreator extends Thread {
	
  private HashSet<String> jarfiles = new HashSet<String>();
  private JarOutputStream extendedJarStream = null;
  private File extendedJarPath = null;
  private Queue<File> jarFileQueue = new LinkedBlockingQueue<File>();
  private boolean working = false;
  
  {
  	//This file is created when the Outputstream gets initialized
  	jarfiles.add("META-INF/MANIFEST.MF");
  }
  
	public void addExtensionJar(File jar) {
		//System.out.println(System.nanoTime() + ": start jar copying");
	   synchronized(jarFileQueue) {
	  	 jarFileQueue.add(jar);
	  	 working = true;
	  	 jarFileQueue.notifyAll();
	    }
	}
	
	@Override
	public void run() {
		while(true) {
			File jar = null;
			synchronized(jarFileQueue) {
	      while (jarFileQueue.isEmpty()) {
	       	try {
	       		working = false;
	       		jarFileQueue.notifyAll();
	       		//System.out.println(System.nanoTime() + ": End jar copying");
						jarFileQueue.wait();
					} catch (InterruptedException e) {
					}
	      }
	      jar = jarFileQueue.remove();
	    }
      copyExtensionJar(jar);
		}
	}
	
  /**
   * @return
   * @throws IOException 
   */
  public File getExtensionJar() throws IOException
  {
  	synchronized(jarFileQueue) {
  		while(working) {
  			try {
					jarFileQueue.wait();
				} catch (InterruptedException e) {
				}
  		}
  		
    	//Finalize file before returning the handle
    	if(extendedJarStream!=null) {
    		extendedJarStream.close();
    		extendedJarStream = null;
    	}
  	}
  	
    return extendedJarPath;
  }
  
  private void copyExtensionJar(File jar) {
	  
		JarOutputStream jout = getJarOutputStream();
		try {
			JarInputStream jin = new JarInputStream(new FileInputStream(jar));
			copyJarFile(jin, jout);
		} catch (IOException ex) {
			BaseUtil.LOG.error("Error copying jar", ex);
			throw new RuntimeException(ex);
		}
  }
  
	/**
	 * Copies all files from JarInputStream to JarOutputStream which are not
	 * already contained in JarOutputStream
	 * 
	 * @param jin		stream from which the files are copied
	 * @param jout	target location for input files
	 * @throws IOException
	 */
	private void copyJarFile(JarInputStream jin, JarOutputStream jout)
			throws IOException {
		ZipEntry entry;
		byte[] chunk = new byte[32768];
		int bytesRead;
		int bytesWritten = 0;
		
		while ((entry = jin.getNextEntry()) != null) {
			if (!jarfiles.contains(entry.getName())) {
				try {
					jarfiles.add(entry.getName());
					// Add file entry to output stream (meta data)
					ZipEntry newZe = new ZipEntry(entry); // fix for ZipEntry exception?
					newZe.setCompressedSize(-1);
					jout.putNextEntry(newZe);
					// Copy data to output stream (actual data)
					bytesWritten = 0;
					if (!entry.isDirectory()) {
						while ((bytesRead = jin.read(chunk)) != -1) {
							jout.write(chunk, 0, bytesRead);
							bytesWritten += bytesRead;
						}
					}
					jout.closeEntry();
				} catch (ZipException ex) {
					BaseUtil.LOG.info(entry.getName() + " wrote:"+bytesWritten);
					ex.printStackTrace();
				}
			} else {
				//TODO: Debug log print which files were blocked
				//BaseUtil.LOG.info("blocked " + entry.getName());
			}
		}
	}
	
	private JarOutputStream getJarOutputStream() {
		//If we have a existing jar stream just use it
		if(extendedJarStream != null) {
			return extendedJarStream;
		}
		
	  //Otherwise create a new jar and outputstream in which new jars
		//can be appended
		File baseJar = null;
		if(extendedJarPath != null) {
			baseJar = extendedJarPath;
		} else {
			JobConf job = new JobConf();
			job.setJarByClass(JaqlUtil.class);
			String original = job.getJar();
			if(original != null) {
				baseJar = new File(original);
			}
		}
		
		//Creat new temp jaql file
		File tmpDir = new File(System.getProperty("java.io.tmpdir")
				+ File.separator + "jaql_" + System.nanoTime());
		tmpDir.mkdir();
		// TODO: figure out why this causes occasional thread dumps on linux
		//tmpDir.deleteOnExit(); 
		
		extendedJarPath = new File(tmpDir.getAbsoluteFile() + File.separator +
		 "jaql.jar");
		BaseUtil.LOG.info("creating new jaql.jar: " + extendedJarPath +", starting from: " + baseJar);
		//Copy files over into new file
		try {
			JarOutputStream jout = null;
			if(baseJar != null) {
				JarInputStream jin = new JarInputStream(new FileInputStream(baseJar));
				FileOutputStream fout = new FileOutputStream(extendedJarPath);
				Manifest man = jin.getManifest();
				jout = man == null ? new JarOutputStream(fout) : new JarOutputStream(fout, man);
				copyJarFile(jin, jout);
			} else {
				jout = new JarOutputStream(new FileOutputStream(extendedJarPath));
			}
			extendedJarStream = jout;
		} catch (IOException e) {
			BaseUtil.LOG.error("Error creating jar: " + e);
			throw new RuntimeException(e);
		}
		
		return extendedJarStream;
	}
}
