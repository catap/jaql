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
