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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.registry.FunctionStore;
import com.ibm.jaql.lang.registry.RNGStore;
import com.ibm.jaql.util.PagedFile;

/**
 * 
 */
public class JaqlUtil
{
  public final static JString emptyString = new JString();

  /**
   * @param item
   * @return
   * @throws Exception
   */
  public static boolean ebv(Item item) throws Exception
  {
    JBool b = (JBool) item.get();
    if (b != null)
    {
      return b.value;
    }
    return false;
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

  private static final int       tempPageSize           = 64 * 1024;    // TODO: this is tuneable
  // TODO: This stuff has to be wrapped up into a Session object, and 
  // we need a way to find it from the current Thread.
  private static PagedFile       queryPagedFile;
  private static PagedFile       sessionPagedFile;
  private static Env             sessionEnv             = new Env();
  private static Context         sessionContext         = new Context();
  private static FunctionStore   functionStore;
  private static RNGStore        rngStore;

  /**
   * @return
   */
  public static FunctionStore getFunctionStore()
  {
    if (functionStore == null)
    {
      functionStore = new FunctionStore(
          new FunctionStore.DefaultRegistryFormat());
    }
    return functionStore;
  }

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
   * @param prefix
   * @return
   */
  private static PagedFile makeTempPagedFile(String prefix)
  {
    try
    {
      File f = File.createTempFile(prefix, "dat");
      f.deleteOnExit();
      RandomAccessFile file = new RandomAccessFile(f, "rw");
      file.setLength(0);
      PagedFile pf = new PagedFile(file.getChannel(), tempPageSize);
      return pf;
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /**
   * @return
   */
  public static PagedFile getQueryPageFile()
  {
    if (queryPagedFile == null)
    {
      queryPagedFile = makeTempPagedFile("jaqlQueryTemp");
    }
    return queryPagedFile;
  }

  /**
   * @return
   */
  public static PagedFile getSessionPageFile()
  {
    if (sessionPagedFile == null)
    {
      sessionPagedFile = makeTempPagedFile("jaqlSessionTemp");
    }
    return sessionPagedFile;
  }

  /**
   * @return
   */
  public static Env getSessionEnv()
  {
    return sessionEnv;
  }

  /**
   * @return
   */
  public static Context getSessionContext()
  {
    return sessionContext;
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

}
