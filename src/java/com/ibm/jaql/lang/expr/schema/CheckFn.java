package com.ibm.jaql.lang.expr.schema;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/** Checks whether the first argument matches the schema given in the second argument. If so,
 * returns the first argument. Otherwise, throws an expection.  
 */
public class CheckFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("check", CheckFn.class);
    }
  }
  
  public CheckFn(Expr[] exprs)
  {
    super(exprs);
  }

  public CheckFn(Expr inExpr, Expr schemaExpr)
  {
    super(inExpr, schemaExpr);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public Schema getSchema()
  {
    try 
    {
      if (exprs[1].isCompileTimeComputable().always())
      {
        JsonSchema s = (JsonSchema)exprs[1].compileTimeEval();
        return s.get();
      }
    }
    catch (Exception e) {
      // ignore
    }
    return SchemaFactory.anySchema();
  }
  
  @Override
  protected JsonValue evalRaw(Context context) throws Exception
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
      throw new IllegalArgumentException("input value " + value + " does not match the provided schema");
    }
  }
}
