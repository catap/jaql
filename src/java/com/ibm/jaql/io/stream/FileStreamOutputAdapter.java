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
package com.ibm.jaql.io.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 
 */
public class FileStreamOutputAdapter extends StreamOutputAdapter
{

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.stream.StreamOutputAdapter#openStream(java.lang.String)
   */
  @Override
  protected OutputStream openStream(String location) throws Exception
  {
    File f = null;
    if (location.startsWith("file"))
    {
      URI uri = new URI(location);
      f = new File(uri);
    }
    else
    {
      f = new File(location);
    }
    return new FileOutputStream(f);
  }

}
