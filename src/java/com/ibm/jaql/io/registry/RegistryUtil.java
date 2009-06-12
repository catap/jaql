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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.mapred.JobConf;

/**
 * 
 */
public class RegistryUtil
{

  /**
   * @param <K>
   * @param <V>
   * @param fileName
   * @param registry
   * @throws Exception
   */
  public static <K, V> void readFile(String fileName, Registry<K, V> registry)
      throws Exception
  {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    URL loc = cl.getResource(fileName);

    InputStream input = loc.openStream();
    registry.readRegistry(input);
    input.close();
  }

  /**
   * @param <K>
   * @param <V>
   * @param conf
   * @param name
   * @param registry
   * @throws Exception
   */
  public static <K, V> void readConf(JobConf conf, String name,
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

  /**
   * @param <K>
   * @param <V>
   * @param fileName
   * @param registry
   * @throws Exception
   */
  public static <K, V> void writeFile(String fileName, Registry<K, V> registry)
      throws Exception
  {
    OutputStream ostr = new FileOutputStream(fileName);
    registry.writeRegistry(ostr);
    ostr.close();
  }

  /**
   * @param <K>
   * @param <V>
   * @param conf
   * @param name
   * @param registry
   * @throws Exception
   */
  public static <K, V> void writeConf(JobConf conf, String name,
      Registry<K, V> registry) throws Exception
  {
    ByteArrayOutputStream ostr = new ByteArrayOutputStream();
    registry.writeRegistry(ostr);
    ostr.flush();
    ostr.close();
    conf.set(name, new String(ostr.toByteArray()));
  }
}
