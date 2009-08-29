package com.ibm.jaql.lang.expr.function;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.FunctionLib;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

/** built in functions */
public class BuiltInExpr extends Expr
{
  public BuiltInExpr(Expr ... exprs)
  {
    super(exprs);
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.functionSchema();
  }
  
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }
  
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("builtin(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
  }
  
  @Override
  public BuiltInFunction eval(Context context) throws Exception
  {
    return FunctionLib.getBuiltInFunction(((JsonString)exprs[0].eval(context)).toString());
  }
}
