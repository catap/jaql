package com.ibm.jaql.lang.expr.sql;

import com.ibm.jaql.json.schema.Schema;

public class SqlColumn
{
  protected String id;
  protected Schema schema;
  
  public SqlColumn(String id, Schema schema)
  {
    this.id = id;
    this.schema = schema;
  }

  public Schema getSchema()
  {
    return schema;
  }
}

