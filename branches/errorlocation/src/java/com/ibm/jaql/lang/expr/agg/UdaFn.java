package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

public class UdaFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "uda",
          UdaFn.class,
          new JsonValueParameters(
              new JsonValueParameter("init", SchemaFactory.functionSchema()),
              new JsonValueParameter("accumulate", SchemaFactory.functionSchema()),
              new JsonValueParameter("combine", SchemaFactory.functionSchema()),
              new JsonValueParameter("final", SchemaFactory.functionSchema())),
            SchemaFactory.functionSchema());
    }
  }
  
  public UdaFn(Expr ... exprs)
  {
    super(exprs);
  }
  
  @Override
  public Expr expandRaw(Env env) throws Exception
  {   
    Var var = env.makeVar("$");
    Expr body = new UdaCallFn(new VarExpr(var), exprs[0], exprs[1], exprs[2], exprs[3]);
    return new DefineJaqlFunctionExpr(new Var[] { var }, body);
  }
}
