package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

public class UdaCallFn extends AlgebraicAggregate
{
  private Function initFn;
  private Function accumulateFn;
  private Function combineFn;
  private Function finalFn;
  private JsonValue partial;
  private Context context;

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "udacall",
          UdaCallFn.class,
          new JsonValueParameters(
              new JsonValueParameter("array", SchemaFactory.arrayOrNullSchema()),
              new JsonValueParameter("init", SchemaFactory.functionSchema()),
              new JsonValueParameter("accumulate", SchemaFactory.functionSchema()),
              new JsonValueParameter("combine", SchemaFactory.functionSchema()),
              new JsonValueParameter("final", SchemaFactory.functionSchema())),
            SchemaFactory.anySchema());
    }
  }
  
  public UdaCallFn(Expr ... args)
  {
    super(args);
  }
  
  @Override
  public void init(Context context) throws Exception
  {
    this.context = context;
    initFn = (Function)exprs[1].eval(context);
    accumulateFn = (Function)exprs[2].eval(context);
    combineFn = (Function)exprs[3].eval(context);
    finalFn = (Function)exprs[4].eval(context);
    
    initFn.setArguments(new Expr[0]);
    partial = initFn.eval(context);
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    accumulateFn.setArguments(partial, value);
    partial = accumulateFn.eval(context);
  }
  
  @Override
  public void combine(JsonValue value) throws Exception
  {
    combineFn.setArguments(partial, value);
    partial = combineFn.eval(context);
  }


  @Override
  public JsonValue getPartial() throws Exception
  {
    return partial;
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    finalFn.setArguments(partial);
    return finalFn.eval(context);
  }
}
