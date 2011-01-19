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
package com.ibm.jaql.lang.expr.index;

interface HashtableConstants
{
  public final static byte GET_CMD           =  0;
  public final static byte GETN_CMD          =  1;
  public final static byte USE_CMD           =  2;
  public final static byte RELEASE_CMD       =  3;
  
  public final static byte OK_CMD            =  5;
  public final static byte FOUND_CMD         =  6;
  public final static byte NOT_FOUND_CMD     =  7;
  public final static byte BUILD_CMD         =  8;
  public final static byte SCHEMA_CMD        =  9;
  public final static byte PUT_CMD           = 10;
}

