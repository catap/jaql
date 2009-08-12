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

/** Base class for all number JSON values. */
public abstract class JsonNumber extends JsonNumeric
{
  public final static JsonLong ZERO           = new JsonLong(0);
  public final static JsonLong ONE            = new JsonLong(1);
  public final static JsonLong MINUS_ONE      = new JsonLong(-1);
}
