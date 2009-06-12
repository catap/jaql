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
package com.ibm.jaql.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

// TODO: this should really work on Readers instead of InputStreams
/**
 * 
 */
public class TeeInputStream extends InputStream
{
  InputStream  in;
  OutputStream out;

  /**
   * @param in
   * @param out
   */
  public TeeInputStream(InputStream in, OutputStream out)
  {
    this.in = in;
    this.out = out;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#read()
   */
  @Override
  public int read() throws IOException
  {
    int x = in.read();
    if (x >= 0)
    {
      out.write(x);
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    len = read(b, off, len);
    out.write(b, off, len);
    return len;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#available()
   */
  @Override
  public int available() throws IOException
  {
    return in.available();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#close()
   */
  @Override
  public void close() throws IOException
  {
    in.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#mark(int)
   */
  @Override
  public synchronized void mark(int arg0)
  {
    in.mark(arg0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#markSupported()
   */
  @Override
  public boolean markSupported()
  {
    return in.markSupported();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#reset()
   */
  @Override
  public synchronized void reset() throws IOException
  {
    in.reset();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#skip(long)
   */
  @Override
  public long skip(long arg0) throws IOException
  {
    return in.skip(arg0);
  }

}
