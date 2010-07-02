/*
 * Copyright (C) IBM Corp. 2010.
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

package com.ibm.jaql.util.shell;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.Queue;
 

public class ChainedReader extends Reader
{
  protected Queue<Reader> readers = new ArrayDeque<Reader>();
  protected char[] onechar = new char[1];
  

  public ChainedReader()
  {
  }

  public ChainedReader(Object lock)
  {
    super(lock);
  }

  public void add(Reader reader)
  {
    readers.add(reader);
  }
  
  @Override
  public boolean ready() throws IOException
  {
    return (!readers.isEmpty()) && readers.peek().ready();
  }

  @Override
  public void close() throws IOException
  {
    while( !readers.isEmpty() )
    {
      readers.remove().close();
    }
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException
  {
    while( !readers.isEmpty() )
    {
      Reader reader = readers.peek();
      int n = reader.read(cbuf, off, len);
      if( n >= 0 )
      {
        assert n != 0: "read should block until some input is available";
        return n;
      }
      readers.remove().close();
    }
    return -1; // eof
  }

  @Override
  public int read() throws IOException 
  {
    int n = read(onechar, 0, 1);
    if( n == 1 )
    {
      return onechar[0];
    }
    return -1;
  }

}
