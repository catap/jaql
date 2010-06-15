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
import java.io.InputStream;

public class ProcessRunner extends Thread
{
  protected ProcessBuilder procBuilder;
  
  public ProcessRunner(String name, ProcessBuilder procBuilder)
  {
    super("procmon:"+name);
    this.procBuilder = procBuilder;
    super.setDaemon(true);
  }
  
  @Override public void run()
  {
    try
    {
      procBuilder.redirectErrorStream(true);
      Process proc = procBuilder.start();
      InputStream in = proc.getInputStream();
      byte[] buf = new byte[1024];
      while( true )
      {
        int len = in.read(buf);
        if( len < 0 )
        {
          break;
        }
        System.err.write(buf, 0, len);
      }
    }
    catch( IOException e )
    {
      e.printStackTrace();
    }
  }
}

