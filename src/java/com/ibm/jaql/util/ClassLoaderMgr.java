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
import java.net.URL;
import java.net.URLClassLoader;

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

  /**
   * @param names
   * @return
   * @throws Exception
   */
  public static File addExtensionJars(String[] names) throws Exception
  {
    if (names == null || names.length == 0) return null;

    // add the new jars to the classloader
    classLoader = createClassLoader(names);

    // set the current thread's classloader
    Thread.currentThread().setContextClassLoader(classLoader);

    // get this jar
    JobConf job = new JobConf();
    job.setJarByClass(JaqlUtil.class);
    String thisJarName = job.getJar();

    // make the temp directory
    File tmpDir = new File(System.getProperty("java.io.tmpdir")
        + File.separator + "jaql_" + System.nanoTime());
    if (tmpDir.exists())
    {
      tmpDir.delete();
    }
    tmpDir.mkdir();
    tmpDir.deleteOnExit();

    File jarsDir = new File(tmpDir.getAbsolutePath() + File.separator + "jars");
    jarsDir.mkdir();

    // copy this jar and extension jars to temp and unjar them
    unpackJar(thisJarName, jarsDir);

    for (int i = 0; i < names.length; i++)
    {
      unpackJar(new File(names[i]).getAbsolutePath(), jarsDir);
    }

    // jar them into a new file; name = jaql_ts.jar
    extendedJarPath = new File(tmpDir.getAbsoluteFile() + File.separator
        + "jaql.jar");
    packJar(jarsDir, extendedJarPath);

    BaseUtil.LOG.info("jar for mapreduce at: " + extendedJarPath);

    return extendedJarPath;
  }

  /**
   * @param srcFile
   * @param tgtDir
   * @throws Exception
   */
  private static void unpackJar(String srcFile, File tgtDir) throws Exception
  {
    BaseUtil.LOG.info("unpacking jar: " + srcFile + " -> "
        + tgtDir.getAbsolutePath() + "...");
    Runtime rt = Runtime.getRuntime();
    Process p = rt.exec(new String[]{"jar", "-xf", srcFile}, null, tgtDir);

    p.waitFor();

    BaseUtil.LOG.info("unpacked!");
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
   * @param srcDir
   * @param tgtFile
   * @throws Exception
   */
  private static void packJar(File srcDir, File tgtFile) throws Exception
  {
    BaseUtil.LOG.info("packing final jar from: " + srcDir.getAbsolutePath()
        + " -> " + tgtFile.getAbsolutePath() + "...");
    BaseUtil.LOG.info("jar command: " + "jar " + " -cf "
        + tgtFile.getAbsolutePath() + " * working dir= "
        + srcDir.getAbsolutePath());
    Runtime rt = Runtime.getRuntime();
    Process p = rt.exec(new String[]{"jar", "-cf", tgtFile.getAbsolutePath(),
        "."}, null, srcDir);

    p.waitFor();

    BaseUtil.LOG.info("packed!");
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
