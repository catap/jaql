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
package com.ibm.jaql.lang.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.registry.RNGStore;

/**
 * 
 */
public class JaqlUtil
{
  /** Returns true if the value is a JBool that equals true
   * @param value a JBool
   * @return
   * @throws Exception if the value is not a JBool
   */
  public static boolean ebv(JsonValue value) throws Exception
  {
    JsonBool b = (JsonBool) value;
    if (b != null)
    {
      return b.get();
    }
    return false;
  }

  public final static <T> T enforceNonNull(T v)
  {
    if (v == null)
    {
      throw new NullPointerException("value must not be null");
    }
    return v;
  }

  //  private static HashMap<Seekable, Long> fileIdMap = new HashMap<Seekable, Long>();
  //  private static ArrayList<Seekable> files = new ArrayList<Seekable>();
  //
  //  public static synchronized long assignFileId(Seekable file)
  //  {
  //    Long fileId = fileIdMap.get(file);
  //    if( fileId == null )
  //    {
  //      fileId = new Long(files.size());
  //      fileIdMap.put(file, fileId);
  //      files.add(file);
  //    }
  //    return fileId.longValue();
  //  }
  //
  //  public static Seekable findFile(long fileId)
  //  {
  //    return files.get((int)fileId);
  //  }
  //
  //  private static HashMap<String, Seekable> filenameMap = new HashMap<String, Seekable>();
  //  // TODO: need to make my own file handles that share buffering and os file handle.
  //  public static Seekable openFileForRead(String filename)
  //    throws IOException
  //  {
  //    Seekable file = filenameMap.get(filename);
  //    if( file == null )
  //    {
  //      file = new SeekableFile(filename, "r");
  //      filenameMap.put(filename, file);
  //    }
  //    return file;
  //  }

  /**
   * 
   */
  public final static PathFilter sequenceFilePartFilter = new PathFilter() {
                                                          public boolean accept(
                                                              Path p)
                                                          {
                                                            String name = p
                                                                .getName();
                                                            return name
                                                                .startsWith("part-")
                                                                || name
                                                                    .startsWith("tip_");
                                                          }
                                                        };

  private static RNGStore        rngStore;

  /**
   * @return
   */
  public static RNGStore getRNGStore()
  {
    if (rngStore == null)
    {
      rngStore = new RNGStore(new RNGStore.DefaultRegistryFormat());
    }
    return rngStore;
  }

  /**
   * @return
   */
  public static AdapterStore getAdapterStore()
  {
    // static is managed by AdapterStore
    AdapterStore store = AdapterStore.getStore();
    if (store == null)
    {
      // use the default format-- replace this method if you want to use a custom format
      store = AdapterStore.initStore();
    }
    return store;
  }



  /**
   * @param varSet1
   * @param varSet2
   * @return
   */
  public static boolean allIn(HashSet<Var> varSet1, HashSet<Var> varSet2)
  {
    for (Var v : varSet1)
    {
      if (varSet2.contains(v))
      {
        return false;
      }
    }
    return true;
  }

  public static <T> List<T> toList(T[] array)
  {
    List<T> result = new ArrayList<T>(array.length);
    for (T v : array) result.add(v);
    return result;
  }
  
  public static <T> List<T> toUnmodifiableList(T[] array)
  {
    return Collections.unmodifiableList(toList(array));
  }
  
  public static RuntimeException rethrow(Exception e)
  {
    if (e instanceof RuntimeException)
    {
      return (RuntimeException)e;
    }
    else
    {
      return new RuntimeException(e);
    }
  }
}
