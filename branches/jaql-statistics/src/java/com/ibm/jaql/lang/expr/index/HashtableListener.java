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
package com.ibm.jaql.lang.expr.index;

import java.io.IOException;
import java.net.Socket;

import com.ibm.jaql.util.AbstractSocketListener;

public class HashtableListener extends AbstractSocketListener
{
  public HashtableListener(int port, int timeToLive)
  {
    super(port, timeToLive);
    logger.info("starting hashtable server on port="+port);
  }

  public Runnable makeServer(Socket socket) throws IOException
  {
    return new HashtableServer(socket);
  }

  public static void main(String... args)
  {
    int port = Integer.parseInt(args[0]);
    int timeToLive = Integer.parseInt(args[1]);
    new HashtableListener(port, timeToLive).run();
  }
  
}

