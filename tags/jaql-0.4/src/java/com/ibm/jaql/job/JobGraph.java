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
import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JobGraph
{
  protected ArrayList<Stage> stages = new ArrayList<Stage>();
  protected ArrayList<Stage> ready = new ArrayList<Stage>();
  protected HashSet<Stage> running = new HashSet<Stage>();
  protected int clock = 0;
  protected Throwable error = null;
  protected boolean compiled = false;
  protected int numComplete;

  /**
   * return true if x is a predecessor of y in the graph.
   * @param x
   * @param y
   */
  private final boolean reaches1(Stage x, Stage y)
  {
    for( Stage z: x.next )
    {
      if( z == y ) 
      {
        return true;
      }
      else if( z != null && z.clock < x.clock )
      {
        z.clock = x.clock;
        if( reaches1(z,y) )
        {
          return true;
        }
      }
    }
    return false;
  }

  public final boolean reaches(Stage x, Stage y)
  {
    x.clock = ++clock;
    return reaches1(x,y);
  }

  /**
   * Compute the transitive reduction of the graph to eliminate redundant edges.
   */
  protected void minimizeGraph()
  {
    for( Stage x: stages )
    {
      if( reaches(x,x) )
      {
        throw new RuntimeException("cycles are not permitted in flow graph");
      }
      for(int i = 0 ; i < x.next.size() ; )
      {
        Stage y = x.next.get(i);
        x.next.set(i, null);
        if( reaches(x,y) )
        {
          x.next.remove(i);
        }
        else
        {
          x.next.set(i, y);
          i++;
        }
      }
    }
  }
  
  protected void reset()
  {
    error = null;
    numComplete = 0;
    ready.clear();
    for(Stage x: stages)
    {
      x.reset();
    }
    for(Stage x: stages)
    {
      for( Stage y: x.next )
      {
        y.waitCount++;
      }
    }
    for(Stage x: stages)
    {
      if( x.waitCount == 0 )
      {
        ready.add(x);
      }
    }
  }

  public void compile() throws Exception
  {
    minimizeGraph();
    for(Stage x: stages)
    {
      x.compile();
    }
    compiled = true;
  }

  public synchronized void run(Executor executor) throws Exception
  {
    if( !compiled )
    {
      compile();
    }
    assert ready.isEmpty();
    reset();
    while( true )
    {
      if( error != null )
      {
        ready.clear();
        // TODO: kill running jobs? cleanup unused results?
      }
      else if( ! ready.isEmpty() )
      {
        Stage x = ready.remove(0);
        running.add(x);
        try
        {
          x.start(executor);
        }
        catch( Throwable t )
        {
          this.error = t;
        }
      }
      else if( ! running.isEmpty() )
      {
        this.wait(); // TODO: use timeout?
      }
      else // we're done: nothing running, nothing ready to run
      {
        break;
      }
    }
    if( error != null )
    {
      throw new RuntimeException("job failed: ", error);
    }
    if( numComplete != stages.size() )
    {
      throw new RuntimeException("we lost track of stages: "+numComplete+" != "+stages.size());
    }
  }

  protected synchronized void complete(Stage stage, Throwable error)
  {
    numComplete++;
    this.notifyAll();
    if( ! running.remove(stage) )
    {
      // This should never happen!
      RuntimeException e = new RuntimeException("The graph has been corrupted! (non-running stage completed)");
      this.error = e; 
      throw e;
    }
    if( error != null )
    {
      System.err.println("stage completed with error: "+error);
      this.error = error;
    }
    else if( this.error == null ) // Don't schedule more if we hit an error
    {
      // Add any stages that are now ready.
      for(Stage x: stage.next)
      {
        x.waitCount--;
        if( x.waitCount <= 0 )
        {
          if( x.waitCount < 0 )
          {
            // This should never happen!
            RuntimeException e = new RuntimeException("The graph has been corrupted! (invalid waitCount)");
            this.error = e; 
            throw e;
          }
          else
          {
            ready.add(x);
          }
        }
      }
    }
  }
  
  private static class DumbStage extends Stage
  {
    private String id;

    protected DumbStage(JobGraph graph, String id)
    {
      super(graph);
      this.id = id;
    }

    @Override
    public void runStage() throws Exception
    {
      System.out.println("starting "+id);
      Thread.sleep(1000+(int)(Math.random()*5000));
      System.out.println("ending "+id);
    }
  }
  
  public static void main(String av[]) throws Exception
  {
    JobGraph g = new JobGraph();
    Stage a = new DumbStage(g, "a");
    Stage b = new DumbStage(g, "b");
    Stage c = new DumbStage(g, "c");
    Stage ab = new DumbStage(g, "ab");
    Stage ac = new DumbStage(g, "ac");
    Stage bc = new DumbStage(g, "bc");
    Stage abc = new DumbStage(g, "abc");
    a.before(ab);
    a.before(ac);
    b.before(ab);
    b.before(bc);
    c.before(ac);
    c.before(bc);
    ab.before(abc);
    ac.before(abc);
    bc.before(abc);
    b.before(abc);
    //Executor executor = new Executor() { public void execute(Runnable r) { r.run(); } };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try
    {
      g.run(executor);
    }
    finally
    {
      executor.shutdown();
    }
    System.out.println("\ndone!");
  }
}
