package com.ibm.jaql.lang.expr.schema;

import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/** Returns its first argument and adds the schema information given in the second argument
 * without validation. Use carefully!   
 */
@JaqlFn(fnName="assert", minArgs=2, maxArgs=2)
public class AssertFn extends CheckFn
{
  public AssertFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonSchema schemaValue = (JsonSchema)exprs[1].eval(context);
    if (schemaValue == null)
    {
      throw new IllegalArgumentException("schema argument must not be null");
    }
      
    // no schema checking
    JsonValue value = exprs[0].eval(context);
    return value;
  }
}
