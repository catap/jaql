/*
 * Copyright (C) IBM Corp. 2010.
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
 * A value of an enumeration of JsonString values.
 * 
 * This is often implemented my a Java enum.
 * However, it will also likely be implemented by an
 * EnumSchema instance in the future. 
 */
public interface JsonEnum
{
  JsonString jsonString();
  int ordinal();
}
