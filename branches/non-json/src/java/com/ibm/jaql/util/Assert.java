/*
 * Copyright (C) IBM Corp. 2009.
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

import java.util.Collection;

/**
 * Our own assertion class that is useful for failing fast and for eliminating
 * duplication.
 */
public class Assert {

  /**
   * A private constructor to block instantiation.
   */
  private Assert() {
  }

  /**
   * Asserts that an object is not <tt>null</tt>.
   * 
   * @param o An <code>Object</code> to check.
   * @param message Message to display when the assertion fails.
   * @throws NullPointerException If the assertion fails.
   */
  public static void notNull(Object o, String message) {
    if (o == null) {
      throw new NullPointerException(message);
    }
  }

  /**
   * Asserts that a <code>String</code> is neither empty (i.e. contains at least
   * one non-whitespace character) nor <tt>null</tt>.
   * 
   * @param s A <code>String</code> to check.
   * @param message Message to display when the assertion fails.
   * @throws IllegalArgumentException If <code>s</code> is empty.
   */
  public static void notEmpty(String s, String message) {
    if (s == null || trim(s).equals("")) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Asserts that a <code>Collection</code> is neither empty (i.e. contains at
   * least one element) nor <tt>null</tt>.
   * 
   * @param c A <code>Collection</code> to check.
   * @param message Message to display when the assertion fails.
   * @throws IllegalArgumentException If <code>c</code> is empty.
   */
  public static void notEmpty(Collection<?> c, String message) {
    if (c == null || c.isEmpty()) {
      throw new IllegalArgumentException(message);
    }
  }

  public static void empty(String s, String message) {
    if (s != null && trim(s).equals("")) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Asserts that a condition is <tt>true</tt>.
   * 
   * @param condition A condition to check.
   * @param message Message to display when the assertion fails.
   * @throws IllegalArgumentException If the assertion fails.
   */
  public static void isTrue(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Asserts that a condition is <tt>true</tt>.
   * 
   * @param condition A condition to check.
   * @param message Message to display when the assertion fails.
   * @throws IllegalStateException If the assertion fails.
   */
  public static void validState(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }

  private static String trim(String s) {
    return s.trim();
  }
}
