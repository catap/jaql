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
package com.ibm.jaql.json.type;

import java.io.PrintStream;

/**
 * 
 */
public abstract class JAtom extends JValue
{
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#equals(java.lang.Object)
   */
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /**
   * @return
   */
  public final boolean isAtom()
  {
    return true;
  }

  /**
   * Print the atom on the stream in (extended) JSON text format.
   * 
   * @param out
   * @throws Exception
   */
  @Override
  public void print(PrintStream out) throws Exception
  {
    out.print(this.toJSON());
  }

  // TODO: eliminate clone() from JAtoms; can use JaqlType.copy() or Item.copy() instead.
  // public abstract JAtom clone();

}
