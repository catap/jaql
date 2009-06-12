/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.job;

import java.util.ArrayList;
import java.util.concurrent.Executor;


public abstract class Stage implements Runnable
{
  protected JobGraph graph;
  protected ArrayList<Stage> next = new ArrayList<Stage>();
  protected int clock = 0;
  protected boolean complete = false;
  protected int waitCount;
  
  protected Stage(JobGraph graph)
  {
    this.graph = graph;
    graph.stages.add(this);
  }
  
  public void reset()
  {
    waitCount = 0;
    complete = false;
  }

  public void compile() throws Exception
  {
  }

  public void start(Executor executor)
  {
    executor.execute(this);
  }
  
  public void run()
  {
    try
    {
      runStage();
      graph.complete(this, null);
      return;
    }
    catch(Throwable t)
    {
      t.printStackTrace(); // TODO: log
      graph.complete(this, t);
      return;
    }
    // graph.complete(this, new RuntimeException("we somehow missed the error..."));
  }

  public void before(Stage ab)
  {
    if( ! next.contains(ab) )
    {
      next.add(ab);
    }
  }

  public abstract void runStage() throws Exception;

}
