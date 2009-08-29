package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

public final class BuiltInFunction extends Function
{
  private static final Expr[] NO_ARGS = new Expr[0];
  private Expr[] args = NO_ARGS;
  private BuiltInFunctionDescriptor descriptor;
  private JsonValueParameters parameters;
  
  public BuiltInFunction(BuiltInFunctionDescriptor descriptor)
  {
    if (descriptor == null)
    {
      throw new IllegalArgumentException("descriptor must not be null");
    }
    this.descriptor = descriptor;
    this.parameters = descriptor.getParameters();
  }
  
  @Override
  public BuiltInFunction getCopy(JsonValue target)
  {
    return new BuiltInFunction(descriptor);
  }

  @Override
  public Function getImmutableCopy() throws Exception
  {
    return new BuiltInFunction(descriptor);
  }
  
  public BuiltInFunctionDescriptor getDescriptor()
  {
    return descriptor;
  }

  public Expr inline()
  {
    return descriptor.construct(args);
  }

  @Override
  public JsonValueParameters getParameters()
  {
    return parameters;  
  }

  @Override
  public void prepare(int numArgs)
  {
    if (args.length != numArgs)
    {
      args = new Expr[numArgs];
    }
  }

  @Override
  protected void setArgument(int pos, JsonValue value)
  {
    args[pos] = new ConstExpr(value);
  }

  @Override
  protected void setArgument(int pos, JsonIterator it)
  {
    // TODO avoid copying when possible
    try 
    {
      SpilledJsonArray a = new SpilledJsonArray();
      a.addCopyAll(it);
      args[pos] = new ConstExpr(a);
    }
    catch (Exception e)
    {
      JaqlUtil.rethrow(e);
    }
  }

  @Override
  protected void setArgument(int pos, Expr expr)
  {
    args[pos] = expr;
  }

  @Override
  protected void setDefault(int pos)
  {
    setArgument(pos, parameters.defaultOf(pos));
  }
  
  Expr[] getArgs()
  {
    return args;
  }
}
