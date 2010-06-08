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
package com.ibm.jaql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

/**
 * 
 */
public class TestSpillFile
{
  String           filename = System.getProperty("test.cache.data") + File.separator + "spillfile.dat";
  RandomAccessFile file;
  PagedFile        pfile;
  SpillFile        spill;

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception
  {
    File f = new File(filename);
    f.deleteOnExit();
    file = new RandomAccessFile(f, "rw");
    file.setLength(0);
    pfile = new PagedFile(file.getChannel(), 128);
    spill = new SpillFile(pfile);
  }
  
  @Test
  public void testSpillJsonArray() throws Exception {
	  ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
	  DataOutput out = new DataOutputStream(byteOut);
	  
	  //Prepare a array with data and serialize it
	  SpilledJsonArray original = new SpilledJsonArray();
	  original.add(new JsonString("test"));
	  original.add(new JsonString("test"));
	  original.add(new JsonString("test"));
	  original.add(new JsonString("test"));
	  original.getSpillSerializer().write(out, original);
	  
	  //Initialize a test array with a cache size of 0 so that the values should be
	  //immediately written to the spill file.
	  SpilledJsonArray testArr = new SpilledJsonArray(0);
	  //Use the serialized data as input for testing
	  DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
	  
	  //Copy the serialized data, and test if it created a spill file
	  testArr.addCopySerialized(in, testArr.getSpillSerializer());
	  assertTrue(testArr.hasSpillFile());
  }

  /**
   * @throws Exception
   */
  @Test
  public void scribble() throws Exception
  {
    spill.clear();
    byte[] buf = new byte[10];
    byte[] buf2 = new byte[10];
    for (int i = 0; i < buf.length; i++)
    {
      buf[i] = (byte) i;
    }
    long N = 1000;
    for (long i = 0; i < N; i++)
    {
      spill.write(buf, 2, 3);
      spill.writeLong(i);
    }
    spill.freeze();
    SpillFile.SFDataInput in = spill.getInput();
    for (int repeat = 0; repeat < 3; repeat++)
    {
      long i = 0;
      long j = 0;
      try
      {
        while (true)
        {
          in.readFully(buf2, 2, 3);
          for (int k = 2; k < 5; k++)
          {
            assertEquals(buf2[k], buf[k]);
          }
          j = in.readLong();
          assertEquals(i, j);
          i++;
        }
      }
      catch (EOFException ex)
      {
        assertEquals(i, N);
      }
      in.rewind();
    }
  }

  /**
   * @throws Exception
   */
  @After
  public void tearDown() throws Exception
  {
  }

}
