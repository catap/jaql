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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class ClassLoaderMgr
{

  private static File        extendedJarPath = null;
  private static ClassLoader classLoader     = null;
  private static HashSet<String> jarfiles = new HashSet<String>();

  /**
   * @param names
   * @return
   * @throws Exception
   */
  public static File addExtensionJars(String[] names) throws Exception
  {
	if (names == null || names.length == 0)
		return null;

	// add the new jars to the classloader
	classLoader = createClassLoader(names);
	
	// set the current thread's classloader
	Thread.currentThread().setContextClassLoader(classLoader);
	
	combineJars(names);

	BaseUtil.LOG.info("jar for mapreduce at: " + extendedJarPath);
	return extendedJarPath;
  }

	/**
	 * Combines the extension jars and the original jar into one jar. When several
	 * jars contain the same file/class the first version is used. The order in
	 * which the jars are inserted into the new jar is first jaql, then the
	 * extensions in the order in which they were defined.
	 * 
	 * @param extensions paths of the extension jars
	 */
	private static void combineJars(String[] extensions) {
		File tmpDir = new File(System.getProperty("java.io.tmpdir")
				+ File.separator + "jaql_" + System.nanoTime());
		tmpDir.mkdir();
		extendedJarPath = new File(tmpDir.getAbsoluteFile() + File.separator +
		 "jaql.jar");
		
		try {
			extendedJarPath.createNewFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// get the original jaql.jar
		JobConf job = new JobConf();
		job.setJarByClass(JaqlUtil.class);
		String original = job.getJar();
		File jaql = new File(original);

		try {
			//Copy content of jaql.jar to new jar
			JarInputStream jin = new JarInputStream(new FileInputStream(jaql));
			JarOutputStream jout = new JarOutputStream(new FileOutputStream(extendedJarPath),
																									jin.getManifest());
			copyJarFiles(jin, jout);

			//Copy content of all extension jars to new jar
			for (String extension : extensions) {
				jin = new JarInputStream(new FileInputStream(extension));
				copyJarFiles(jin, jout);
			}

			jout.close();
		} catch (IOException ex) {
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
	private static void copyJarFiles(JarInputStream jin, JarOutputStream jout)
			throws IOException {
		ZipEntry entry;
		byte[] chunk = new byte[8192];
		int bytesRead;

		while ((entry = jin.getNextEntry()) != null) {
			if (!jarfiles.contains(entry.getName())) {
				try {
					jarfiles.add(entry.getName());
					// Add file entry to output stream (meta data)
					jout.putNextEntry(entry);
					// Copy data to output stream (actual data)
					if (!entry.isDirectory()) {
						while ((bytesRead = jin.read(chunk)) != -1) {
							jout.write(chunk, 0, bytesRead);
						}
					}
				} catch (ZipException ex) {
					System.out.println(entry.getName());
					ex.printStackTrace();
				}
			} else {
				System.out.println("blocked " + entry.getName());
			}
		}
	}
	
  /**
   * @param paths
   * @return
   * @throws Exception
   */
  private static ClassLoader createClassLoader(String[] paths) throws Exception
  {
    if (paths == null) return null;
    int numPaths = paths.length;
    if (numPaths == 0) return null;

    URL[] urls = new URL[numPaths];
    for (int i = 0; i < numPaths; i++)
    {
      urls[i] = new URL("file:" + paths[i]);
    }
    ClassLoader parent = (classLoader == null)
        ? JsonValue.class.getClassLoader()
        : classLoader;
    return new URLClassLoader(urls, parent);
  }

  /**
   * @return
   */
  public static File getExtensionJar()
  {
    return extendedJarPath;
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

  public static ClassLoader getClassLoader()
  {
    ClassLoader cl = classLoader;
    if( cl == null )
    {
      cl = ClassLoaderMgr.class.getClassLoader();
    }
    return cl;
  }
}
