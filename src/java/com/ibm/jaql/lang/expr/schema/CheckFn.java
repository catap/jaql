package com.ibm.jaql.lang.expr.schema;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/** Checks whether the first argument matches the schema given in the second argument. If so,
 * returns the first argument. Otherwise, throws an expection.  
 * 
 * @author rgemull@us.ibm.com
 */
@JaqlFn(fnName="check", minArgs=2, maxArgs=2)
public class CheckFn extends Expr
{
  public CheckFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    try 
    {
      if (exprs[1].isCompileTimeComputable().always())
      {
        JsonSchema s = (JsonSchema)exprs[1].eval(Env.getCompileTimeContext());
        return s.get();
      }
    }
    catch (Exception e) {
      // ignore
    }
    return SchemaFactory.anyOrNullSchema();
  }
  
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonSchema schemaValue = (JsonSchema)exprs[1].eval(context);
    if (schemaValue == null)
    {
      throw new IllegalArgumentException("schema argument must not be null");
    }
      
    Schema schema = schemaValue.get();
    JsonValue value = exprs[0].eval(context);
    if (schema.matches(value)) 
    {
      return value;
    }
    else
    {
      throw new IllegalArgumentException("input does not match schema");
    }
  }
}
