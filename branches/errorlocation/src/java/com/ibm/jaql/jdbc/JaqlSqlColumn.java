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
package com.ibm.jaql.jdbc;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JaqlSqlColumn
{
  protected final JsonString name;
  protected final int columnIndex;
  protected final JaqlSqlType type;

  public JaqlSqlColumn(JsonString name, int columnIndex, JaqlSqlType type)
  {
    this.name = name;
    this.columnIndex = columnIndex;
    this.type = type;
  }

  public JsonString getJsonName()
  {
    return name;
  }

  public String getName()
  {
    return name.toString();
  }

  public int getIndex()
  {
    return columnIndex;
  }

  public JaqlSqlType getType()
  {
    return type;
  }

  public JsonValue getValue(JsonRecord rec)
  {
    return rec.get(name);
  }

}
