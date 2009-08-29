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
package com.ibm.jaql.json.util;

import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Iterator;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** Iterator over a list of {@link JsonValue}s. This iterator is meant to be accessed either 
 * via the {@link #moveNext()} and {@link #current()} methods or via 
 * <code>foreach</code> statements.
 * 
 * <code>JsonIterator</code> implements {@link Iterator} and {@link Iterable} for convenience, 
 * but it diverts slightly from the contract of {@link Iterator}. In particular, {@link #hasNext()} 
 * moves to the the next value, while {@link #next()} returns this value without moving. 
 * <code>JsonIterator</code> behaves like <code>Iterator</code> when the calling sequence is 
 * <code>hasNext()</code>, <code>next()</code>, <code>hasNext()</code>, <code>next()</code>, ...; 
 * it behaves differently on other call sequences. 
 */
public abstract class JsonIterator implements Iterator<JsonValue>, Iterable<JsonValue>
{
  // -- constants ---------------------------------------------------------------------------------
  
  /** Iterator that does not produce elements */
  public static final JsonIterator EMPTY = EmptyIterator.getInstance(); 
  
  /** 
   * The one and only Iterator that is null.
   * 
   * A null will act like an empty array if iterated. 
   * To detect null, check for NULL:
   *  
   *     if( iter.isNull() ) ...  
   */
  public static final JsonIterator NULL   = NullIterator.getInstance();

  
  // -- constructors ------------------------------------------------------------------------------

  protected JsonIterator() {
  }
  
  protected JsonIterator(JsonValue initialValue) {
    this.currentValue = initialValue;
  }
  
  
  // -- protected variables -----------------------------------------------------------------------

  /** The current value as returned by {@link #current()}. */
  protected JsonValue currentValue;
  
  
  //-- Jaql-style iteration -----------------------------------------------------------------------
  
  /** Returns the current value if the last call to {@link #moveNext()} or {@link #moveN()}
   * returned <code>true</code>. Otherwise, the return value is undefined and no exception
   * is produced. */
  public final JsonValue current() {
    return currentValue;
  }
  
  /** Moves to the next value, if any, and sets {@link #currentValue} to this value. 
   * 
   * @return whether there has been a next value 
   * @throws Exception
   */
  public abstract boolean moveNext() throws Exception;
  
  /** Moves to the next non-null value, if any, and sets {@link #currentValue} to this value. 
   * 
   * @return whether there has been a next non-null value 
   * @throws Exception
   */
  public boolean moveNextNonNull() throws Exception
  {
    while (moveNext())
    {
      if (currentValue != null) 
      {
        return true;
      }
    }
    return false;
  }
  
  /** Moves to the <code>n</code>-th value, if any, and sets {@link #currentValue} to this value. The 
   * standard implementation simply performs <code>n</code> calls to {@link #next()}. If 
   * <code>n<=0</code>, this method does nothing and returns true;
   * 
   * @param n value to move to
   * @return if <code>n>0</code>, whether there has been an <code>n</code>-th value
   * @throws Exception
   */
  public boolean moveN(long n) throws Exception
  {
    for (long i=0; i<n; i++) {
      if (!moveNext()) return false;
    }
    return true;
  }
  
  /** 
   * Returns <code>true</code> when this iterator is <code>null</code>. If this iterator is null,
   * it will not produce any values. This is a convenience method that avoids checks 
   * for <code>null</code> in cases where the caller does not need to distinguish 
   * between empty iterators and null iterators.
   * 
   * @return whether this iterator is <code>null</code>
   */
  public final boolean isNull()
  {
    return this == NULL;
  }

  // -- Java-style interation ---------------------------------------------------------------------
  
  /** Equivalent to {@link JsonIterator#moveNext()}. This method is marked deprecated because it 
   * diverts slightly from the standard Java iterator contract. It will not be removed in 
   * future versions. */
  @Deprecated
  public final boolean hasNext() {
    try
    {
      return moveNext();
    } catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  /** Equivalent to {@link JsonIterator#current()}. This method is marked deprecated because it 
   * diverts slightly from the standard Java iterator contract. It will not be removed in 
   * future versions. */
  @Deprecated
  public final JsonValue next() {
    return currentValue;
  }
  
  /** Unsupported.
   * 
   * @throws UnsupportedOperationException always */
  @Deprecated
  public final void remove() {
    throw new UnsupportedOperationException();
  }
  
  /** Returns this object. Convenience methods that allows using <code>for</code> loops. 
   * 
   * This method is marked deprecated because the returned iterator diverts slightly from the 
   * standard Java iterator contract. This method will not be removed in future versions. 
   */
  @Deprecated
  public final Iterator<JsonValue> iterator() {
    return this;
  }
  
  //-- Utility methods --------------------------------------------------------------------------
  
  /**
   * @param out
   * @throws Exception
   */
  public final void print(PrintStream out) throws Exception
  {
    this.print(out, 0);
  }

  /**
   * 
   * <no indent> [ <indent+2> value, ... <indent> ]
   * 
   * OR
   * 
   * <no indent> []
   * 
   * @param out
   * @param indent
   * @throws Exception
   */
  public final void print(PrintStream out, int indent) throws Exception
  {
    if (this.isNull())
    {
      out.println("null");
    }
    else
    {
      String sep = "";
      out.print("[");
      indent += 2;
      for (JsonValue value : this)
      {
        out.println(sep);
        for (int s = 0; s < indent; s++)
        {
          out.print(' ');
        }
        JsonUtil.print(out, value, indent);
        sep = ",";
      }
      if (sep.length() > 0) // if not empty array
      {
        out.println();
        for (int s = 2; s < indent; s++)
          out.print(' ');
      }
      out.print("]");
    }
  }
}
