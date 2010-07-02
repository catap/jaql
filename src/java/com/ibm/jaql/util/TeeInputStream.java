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

// TODO: This has been replaced by EchoedReader
/**
 * InputStream that forwards everything that is read from it to the provided
 * output stream.
 */
public class TeeInputStream extends InputStream {
  private InputStream in;
  private OutputStream out;

  /**
   * Creates a tee input stream with an input stream and an output stream.
   * 
   * @param in input stream to be read from.
   * @param out output stream which content from the input stream will be
   *          forwarded to.
   */
  public TeeInputStream(InputStream in, OutputStream out) {
    this.in = in;
    this.out = out;
  }

  @Override
  public int read() throws IOException {
    int x = in.read();
    if (x >= 0) {
      out.write(x);
    }
    return x;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    len = in.read(b, off, len);
    out.write(b, off, len);
    return len;
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public synchronized void mark(int arg0) {
    in.mark(arg0);
  }

  @Override
  public boolean markSupported() {
    return in.markSupported();
  }

  @Override
  public synchronized void reset() throws IOException {
    in.reset();
  }

  @Override
  public long skip(long arg0) throws IOException {
    return in.skip(arg0);
  }
}
