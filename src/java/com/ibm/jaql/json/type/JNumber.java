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

/**
 * 
 */
public abstract class JNumber extends JNumeric
{
  public final static JLong ZERO           = new JLong(0);
  public final static JLong ONE            = new JLong(1);
  public final static JLong MINUS_ONE      = new JLong(-1);
  public static final Item  ZERO_ITEM      = new Item(ZERO);
  public static final Item  ONE_ITEM       = new Item(ONE);
  public static final Item  MINUS_ONE_ITEM = new Item(MINUS_ONE);
}
