package com.ibm.jaql.lang.expr.function;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.util.ClassLoaderMgr;

/** An expression that constructs a JSON value for a built-in function */
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
		BuiltInFunctionDescriptor descriptor = getDescriptor(context);
  	return new BuiltInFunction(descriptor);
  }
  
  @SuppressWarnings("unchecked")
  private BuiltInFunctionDescriptor getDescriptor(Context context) throws Exception {
  	String cls = ((JsonString)exprs[0].eval(context)).toString();
		Class<? extends BuiltInFunctionDescriptor> c = 
			(Class<? extends BuiltInFunctionDescriptor>) ClassLoaderMgr.resolveClass(cls);
		return c.newInstance(); 
  }
}
