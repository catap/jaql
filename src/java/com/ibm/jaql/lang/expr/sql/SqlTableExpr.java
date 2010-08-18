package com.ibm.jaql.lang.expr.sql;

import java.util.List;

import com.ibm.jaql.json.schema.RecordSchema;

public abstract class SqlTableExpr extends SqlExpr
{
  public abstract List<? extends SqlColumn> columns();
  public abstract RecordSchema getRowSchema();
}

