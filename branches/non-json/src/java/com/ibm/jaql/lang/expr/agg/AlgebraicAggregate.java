package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Expr;

public abstract class AlgebraicAggregate extends Aggregate
{
  // -- construction ------------------------------------------------------------------------------
  
  public AlgebraicAggregate(Expr ... exprs)
  {
    super(exprs);
  }
  
  // -- aggregation -------------------------------------------------------------------------------
  
  public abstract JsonValue getPartial() throws Exception;
  
  public Schema getPartialSchema()
  {
    return SchemaFactory.anySchema();
  }
  
  public abstract void combine(JsonValue value) throws Exception;
  
  public abstract JsonValue getFinal() throws Exception;
}
