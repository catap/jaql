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
package com.ibm.jaql.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;


public abstract class AbstractSocketListener implements Runnable
{
  public final Logger logger = Logger.getLogger(getClass().getName());

  protected int port;
  protected int maxServerIdleMS;
  protected final AtomicInteger numConnections = new AtomicInteger(0);
  
  public AbstractSocketListener(int port, int timeToLive)
  {
    this.port = port;
    this.maxServerIdleMS = timeToLive;
    
    try {
       logger.addAppender(new FileAppender(new SimpleLayout(),"/tmp/"+logger.getName()+".log",false));
    } catch(Exception e) {}

    logger.setLevel(Level.DEBUG);
  }
  
  public void run()
  {
    ServerSocket serverSocket = null;
    int numIdle = 0;
    try {
      serverSocket = new ServerSocket(port);
      
      while( numIdle < 2 )
      {
        try
        {
          assert numConnections.get() >= 0;
          serverSocket.setSoTimeout(this.maxServerIdleMS);
          
          Socket clientSocket = serverSocket.accept();
          numIdle = 0;
          numConnections.incrementAndGet();
          logger.info("client connected. active="+numConnections);

          try
          {
            Runnable server = makeServer(clientSocket);
            new ServerThread(this, clientSocket, server).start();
          }
          catch( IOException e )
          {
            // log error and continue 
            e.printStackTrace();
          }
        }
        catch( SocketTimeoutException e )
        {
          if( numConnections.get() == 0 )
          {
            numIdle++;
            logger.info("server timeout #"+numIdle+" on port="+port);
          }
        }
      }
    }
    catch (IOException e)
    {
      // log error and exit 
      e.printStackTrace();
    }
    finally
    {
      try
      {
        logger.info("stopping server on port="+port);
        if( serverSocket != null )
        {
          serverSocket.close();
        }
      }
      catch( IOException e )
      {
        // ignore
      }
    }
  }

  public void disconnect(ServerThread server)
  {
    assert numConnections.get() > 0;
    numConnections.decrementAndGet();
    logger.info("disconnected client.  remaining clients="+numConnections);
  }
  
  public abstract Runnable makeServer(Socket socket) throws IOException;
}

