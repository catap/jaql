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
package com.ibm.jaql.lang.core;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;

import com.ibm.jaql.lang.util.JaqlUtil;

/** Run-time context, i.e., values for the variables in the environment.
 * 
 */
public class Context
{
  // PyModule pyModule;

  /**
   * 
   */
  public Context()
  {
    if( JaqlUtil.getSessionContext() == null )
    {
//      PySystemState systemState = Py.getSystemState();
//      if (systemState == null)
//      {
//        systemState = new PySystemState();
//      }
//      Py.setSystemState(systemState);
//      pyModule = new PyModule("jaqlMain", new PyStringMap());
    }
  }

  // public PyModule getPyModule() { return JaqlUtil.getSessionContext().pyModule; }
  
  /** Clears the context.
   * 
   */
  public void reset()
  {
    JaqlUtil.getQueryPageFile().clear();
  }

//  /** UNUSED 
//   * @param item
//   * @return
//   * @throws Exception
//   */
//  public Item makeSessionGlobal(Item item) throws Exception
//  {
//    Item global = new Item();
//    if (item.isAtom())
//    {
//      global.copy(item); // TODO: copy should take a pagefile
//    }
//    else // FIXME: this is not doing what it is supposed to do 
//    {
//      PagedFile pf = JaqlUtil.getSessionPageFile(); // TODO: this is BROKEN!
//      SpillFile sf = new SpillFile(pf);
//      item.write(sf);
//      global.readFields(sf.getInput());      
//    }
//    return global;
//  }

  protected ArrayList<Runnable> atQueryEnd = new ArrayList<Runnable>();
  public void doAtQueryEnd(Runnable task)
  {
    atQueryEnd.add(task);
  }
  
  public void endQuery()
  {
    reset();
    for(Runnable task: atQueryEnd)
    {
      try
      {
        task.run();
      }
      catch(Throwable e)
      {
        e.printStackTrace(); // TODO: log
      }
    }
    atQueryEnd.clear();
  }

  public void closeAtQueryEnd(final Closeable resource)
  {
    doAtQueryEnd(new Runnable() {
      @Override
      public void run()
      {
        try
        {
          resource.close();
        }
        catch (IOException e)
        {
          throw new UndeclaredThrowableException(e);
        }
      }
    });
  }
}
