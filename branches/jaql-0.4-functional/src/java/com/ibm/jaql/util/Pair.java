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

/**
 * @param <A>
 * @param <B>
 */
public class Pair<A, B>
{
  public A a;
  public B b;

  /**
   * 
   */
  public Pair()
  {
  }

  /**
   * @param a
   * @param b
   */
  public Pair(A a, B b)
  {
    this.a = a;
    this.b = b;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = (a == null) ? 0 : System.identityHashCode(a);
    result = prime * result + ((b == null) ? 0 : System.identityHashCode(b));
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Pair<A,B> other = (Pair<A,B>) obj;
    return this.a == other.a && this.b == other.b;
  }
}
