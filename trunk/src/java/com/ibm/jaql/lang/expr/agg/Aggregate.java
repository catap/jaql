package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;

public abstract class Aggregate extends Expr
{
  // -- construction ------------------------------------------------------------------------------
  
  public Aggregate(Expr ... exprs)
  {
    super(exprs);
  }
  
  // -- aggregation -------------------------------------------------------------------------------
  
  public abstract void init(Context context) throws Exception;
  public abstract void accumulate(JsonValue value) throws Exception;
  public abstract JsonValue getFinal() throws Exception;

  
  // -- evaluation --------------------------------------------------------------------------------

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * This function is called by the AggregateExpr for computing
   * many aggregates simultaneously.  The argument expression
   * is the entire array to aggregate.
   * 
   * The default way to process the input aggregation set is to
   *    for each item in input
   *     - if item is null
   *         - ignore the item
   *     - otherwise add the item
   * 
   * If you override this function, you need to override processInitial as well.
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    init(context);
    evalInitialized(context);
    return getFinal();
  }
  
  public void evalInitialized(Context context) throws Exception
  {
    JsonIterator it = exprs[0].iter(context);
    for (JsonValue value : it) 
    {
      if (value != null)
      {
        accumulate(value);
      }
    }
  }

}
