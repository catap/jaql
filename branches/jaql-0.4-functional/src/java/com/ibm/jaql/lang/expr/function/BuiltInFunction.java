package com.ibm.jaql.lang.expr.function;

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
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
  public Function getImmutableCopy()
  {
    return new BuiltInFunction(descriptor);
  }
  
  public BuiltInFunctionDescriptor getDescriptor()
  {
    return descriptor;
  }

  public Expr inline(boolean eval)
  {
    if (eval)
    {
      // cloning necessary because object construction changes parent field in expr's
      Expr[] clonedArgs = new Expr[args.length];
      VarMap varMap = new VarMap();
      for (int i=0; i<args.length; i++)
      {
        HashSet<Var> vars = args[i].getCapturedVars();
        for (Var v : vars) 
        {
          varMap.put(v, v);
        }
        clonedArgs[i] = args[i].clone(varMap);
      }
      return descriptor.construct(clonedArgs);
    }
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
      throw JaqlUtil.rethrow(e);
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
  
  public String formatError(String msg)
  {
    return "In call of builtin function " + descriptor.getName() + ": " + msg;
  }
  
  public static BuiltInFunctionDescriptor getDescriptor(Class<? extends Expr> cls) 
  throws InstantiationException, IllegalAccessException
  {
    BuiltInFunctionDescriptor descriptor = null;
    Class<?>[] inners = cls.getDeclaredClasses();
    for (int i = 0; i < inners.length; i++)
    {
      Class<?> c = inners[i];
      if (BuiltInFunctionDescriptor.class.isAssignableFrom(c))
      {
        if (descriptor != null)
        {
          throw new IllegalArgumentException(cls + " contains two descriptor classes");
        }
        descriptor = (BuiltInFunctionDescriptor) c.newInstance();
      }
    }
    if (descriptor == null)
    {
      throw new IllegalArgumentException(cls + " does not have an inner descriptor class");
    }
    return descriptor;
  }
}
