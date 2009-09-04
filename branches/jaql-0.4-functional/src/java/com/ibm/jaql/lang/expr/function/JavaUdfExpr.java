package com.ibm.jaql.lang.expr.function;

import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

/** An expression that constructs a JSON value for a Java UDF */
public class JavaUdfExpr extends Expr {
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
	  public Descriptor()
	  {
	    super("javaudf", JavaUdfExpr.class);
	  }	   
	}
  
  public JavaUdfExpr(Expr ... exprs)
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
  
  @Override
  public JavaUdfFunction eval(Context context) throws Exception {
    JavaUdfFunction f = new JavaUdfFunction(((JsonString)exprs[0].eval(context)).toString());
    return f;
  }
}
