package com.ibm.jaql.lang.expr.number;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;

/** Function that returns null if its input is null, decimal if its input is decimal, double if
 * its input is a number of a different type. Otherwise, fails. */
abstract class AbstractRealFn extends Expr
{
  public AbstractRealFn(Expr ... exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    boolean nullable = in.is(JsonType.NULL).maybe();
    Schema number = SchemaTransformation.restrictToNumberTypes(in);
    if (number == null)
    {
      if (nullable) return SchemaFactory.nullSchema();
      throw new RuntimeException("exp expects number as input");
    }
    
    // compute result schema
    if (number.is(JsonType.DECFLOAT).never())
    {
      return nullable ? SchemaFactory.doubleOrNullSchema() : SchemaFactory.doubleSchema();
    }
    else if (number.is(JsonType.DECFLOAT).always())
    {
      return nullable ? SchemaFactory.decfloatOrNullSchema() : SchemaFactory.decfloatSchema();
    }
    else
    {
      if (nullable)
      {
        return OrSchema.make(SchemaFactory.doubleOrNullSchema(), SchemaFactory.decfloatSchema());
      }
      else
      {
        return OrSchema.make(SchemaFactory.doubleSchema(), SchemaFactory.decfloatSchema());
      }
    }
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }
  
}
