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
package com.ibm.jaql.io.registry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;

import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * 
 */
public class RegistryUtil
{
  // TODO: is there a better place for this?
  /** 
   * Resolve a local file path to an absolute path using the configurable
   * default current working directory. 
   */
  public static File resolveFile(String location) throws URISyntaxException
  {
    if( location.startsWith("file:") )
    {
      URI uri = new URI(location);
      location = uri.getPath();
    }
    File f = new File(location);
    if( ! f.isAbsolute() )
    {
      String cwd = System.getProperty("jaql.local.dir"); // TODO: better place to get this from? Context would be good!
      if( cwd != null )
      {
        f = new File(cwd + File.separatorChar + location);
      }
    }
    return f;
  }

  /** Read a registry from a file.
   * 
   * @param <K>
   * @param <V>
   * @param fileName
   * @param registry
   * @throws Exception
   */
  public static <K, V> void readFile(String fileName, Registry<K, V> registry)
      throws Exception
  {
    URL url = ClassLoaderMgr.getResource(fileName);
    InputStream input;
    if( url != null )
    {
      input = url.openStream();
    }
    else
    {
      File file = resolveFile(fileName);
      input = new FileInputStream(file);
    }
    registry.readRegistry(input);
    input.close();
  }

  /** Read a registry from a Hadoop configuration file.
   * @param <K>
   * @param <V>
   * @param conf
   * @param name
   * @param registry
   * @throws Exception
   */
  public static <K, V> void readConf(Configuration conf, String name,
      Registry<K, V> registry) throws Exception
  {
    String val = conf.get(name);
    if (val != null)
    {
      InputStream input = new ByteArrayInputStream(val.getBytes());
      registry.readRegistry(input);
      input.close();
    }
  }

  /** Write a registry to a file.
   * @param <K>
   * @param <V>
   * @param fileName
   * @param registry
   * @throws Exception
   */
  public static <K, V> void writeFile(String fileName, Registry<K, V> registry)
      throws Exception
  {
    File file = resolveFile(fileName);
    OutputStream ostr = new FileOutputStream(file);
    registry.writeRegistry(ostr);
    ostr.close();
  }

  /** Write a registry to a Hadoop configuration file.
   * 
   * @param <K>
   * @param <V>
   * @param conf
   * @param name
   * @param registry
   * @throws Exception
   */
  public static <K, V> void writeConf(Configuration conf, String name,
      Registry<K, V> registry) throws Exception
  {
    ByteArrayOutputStream ostr = new ByteArrayOutputStream();
    registry.writeRegistry(ostr);
    ostr.flush();
    ostr.close();
    conf.set(name, new String(ostr.toByteArray())); // fixme: String does character conversions!
  }
}
