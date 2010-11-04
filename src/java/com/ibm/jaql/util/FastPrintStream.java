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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

/** 
 * A java.io.PrintStream replacement that efficiently writes to an underlying OutputStream
 * without any synchronization.  Buffer is always provided, so its best not to have any
 * buffering in the OutputStream.  For writing to a char[], FastPrintBuffer is a better choice.
 */
public class FastPrintStream extends FastPrintWriter
{
  public static Charset UTF8 = Charset.forName("UTF-8");
  
  protected OutputStream os;

  public FastPrintStream(OutputStream os)
  {
    this(os, UTF8);
  }

  public FastPrintStream(OutputStream os, Charset cs)
  {
    this(os, cs, 64*1024);
  }

  public FastPrintStream(OutputStream os, Charset cs, int bufferSize)
  {
    super(new OutputStreamWriter(os, cs), bufferSize);
    this.os = os;
  }

  @Override
  public void close() throws IOException
  {
    flush();
    if( os != System.out && os != System.err )
    {
      os.close();
    }
  }
}
