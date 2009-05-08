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
package com.acme.extensions.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 */
public class SeededSplit implements InputSplit
{

  private long       seed;
  private InputSplit child;

  /**
   * 
   */
  public SeededSplit()
  {
  }

  /**
   * @param child
   * @param seed
   */
  public SeededSplit(InputSplit child, long seed)
  {
    this.seed = seed;
    this.child = child;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLength()
   */
  public long getLength() throws IOException
  {
    return child.getLength();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLocations()
   */
  public String[] getLocations() throws IOException
  {
    return child.getLocations();
  }

  /**
   * @return
   */
  public long getSeed()
  {
    return seed;
  }

  /**
   * @return
   */
  public InputSplit getChildSplit()
  {
    return child;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput arg0) throws IOException
  {
    seed = arg0.readLong();
    String cName = arg0.readUTF();
    try
    {
      Class<?> c = Class.forName(cName).asSubclass(InputSplit.class);;
      this.child = (InputSplit) ReflectionUtils.newInstance(c, null);
      this.child.readFields(arg0);
    }
    catch (ClassNotFoundException ce)
    {
      throw new IOException(ce.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput arg0) throws IOException
  {
    arg0.writeLong(seed);
    arg0.writeUTF(child.getClass().getCanonicalName());
    child.write(arg0);
  }

}
