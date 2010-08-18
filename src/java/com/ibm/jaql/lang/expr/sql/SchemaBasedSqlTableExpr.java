package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonType;

public abstract class SchemaBasedSqlTableExpr extends SqlTableExpr 
{
  protected ArrayList<SqlColumn> columns;
  protected RecordSchema rowSchema;
  
  @Override
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    Schema schema = getSchema();
    if( !schema.is(JsonType.ARRAY, JsonType.NULL).always() )
    {
      throw new SQLException("array schema required instead of: "+schema);
    }
    schema = schema.elements();
    if( schema == null )
    {
      throw new SQLException("an empty array cannot be converted to a table");
    }
    schema = SchemaTransformation.compact(schema);
    if( !(schema instanceof RecordSchema) )
    {
      throw new SQLException("sql requires only records instead of:"+schema);
    }
    rowSchema = (RecordSchema)schema;
    int n = rowSchema.noRequiredOrOptional();
    columns = new ArrayList<SqlColumn>(n);
    for(int i = 0 ; i < n ; i++)
    {
      Field f = rowSchema.getFieldByPosition(i);
      String id = f.getName().toString();
      columns.add(new SqlColumn(id, f.getSchema()));
    }
  }
  
  public List<? extends SqlColumn> columns()
  {
    return columns;
  }
  
  public RecordSchema getRowSchema()
  {
    return rowSchema;
  }
}
