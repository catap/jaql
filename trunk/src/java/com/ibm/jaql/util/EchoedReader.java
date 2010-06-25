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
package com.ibm.jaql.util;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public class EchoedReader extends Reader
{
  protected Reader in;
  protected Writer out;
  
  public EchoedReader(Reader in, Writer out)
  {
    this.in = in;
    this.out = out;
  }
  
  @Override
  public int read() throws IOException
  {
    int b = in.read();
    if (b>=0)
    {
      out.write((char)b);
      out.flush(); // TODO: we could avoid this flush once the outside world also uses our writer...
    }
    return b;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException
  {
    len = in.read(cbuf, off, len);
    if( len > 0 )
    {
      out.write(cbuf, off, len);
      out.flush(); // TODO: we could avoid this flush once the outside world also uses our writer...
    }
    return len;
  }

  @Override
  public boolean ready() throws IOException
  {
    return in.ready();
  }
  
  @Override
  public void close() throws IOException
  {
    out.flush();
    in.close();
  }
}
