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

import org.apache.commons.logging.Log;

/**
 * 
 */
public class UtilForTest
{

  /**
   * @param testFileName
   * @param goldFileName
   * @param LOG
   * @return
   * @throws IOException
   */
  public static boolean compareResults(String testFileName,
      String goldFileName, Log LOG) throws IOException
  {
    // use unix 'diff', ignoring whitespace
    Runtime rt = Runtime.getRuntime();
    Process p = rt.exec(new String[]{"diff", "-w", testFileName, goldFileName});
    InputStream str = p.getInputStream();

    byte[] b = new byte[1024];
    int numRead = 0;
    StringBuilder sb = new StringBuilder();
    while ((numRead = str.read(b)) > 0)
    {
      sb.append(new String(b, 0, numRead, "US-ASCII"));
    }
    if (sb.length() > 0)
      LOG.error("\ndiff -w " + testFileName + " " + goldFileName + "\n" + sb);

    return sb.length() == 0;
  }
}
