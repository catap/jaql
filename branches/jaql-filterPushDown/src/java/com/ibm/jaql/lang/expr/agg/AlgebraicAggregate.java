package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.metadata.MappingTable;

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
  
  /**
   * Note: pushing the aggregation below its TransformExpr will cause it to be executed twice instead of once. So do not push it.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  VarExpr ve= new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
	  mt.add(ve, this, false);            
	  return mt;
  }

  public abstract void combine(JsonValue value) throws Exception;
  
  public abstract JsonValue getFinal() throws Exception;
}
