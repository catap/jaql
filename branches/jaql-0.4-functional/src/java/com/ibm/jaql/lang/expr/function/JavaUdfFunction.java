package com.ibm.jaql.lang.expr.function;

import java.util.Arrays;
import java.util.HashSet;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

public class JavaUdfFunction extends Function {
	
	private static final Expr[] NO_ARGS = new Expr[0];
  private Expr[] args = NO_ARGS;
  private JsonValueParameters parameters = null;
	private Class<?> c;
	
	public JavaUdfFunction(Class<?> c) {
		this.c = c;
	}
	
	public JavaUdfFunction(String cls) {
		c = ClassLoaderMgr.resolveClass(cls.toString());
	}

	@Override
	public Function getCopy(JsonValue target) {
		return new JavaUdfFunction(c);
	}

	@Override
	public Function getImmutableCopy() {
		return new JavaUdfFunction(c);
	}

	@Override
	public Parameters<?> getParameters() {
		if (parameters == null) {
			parameters = new JsonValueParameters(new JsonValueParameter("arg", SchemaFactory.anySchema(), true));
			// Notice: Type inference not possible because the argument at the same position in different
			// eval methods could have different types.
			// TODO: Infer number of arguments from eval method
		}
		return parameters;
	}

	@Override
	public Expr inline(boolean eval) {
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
      return new JavaFunctionCallExpr(c, Arrays.asList(clonedArgs));
	  }
		return new JavaFunctionCallExpr(c, Arrays.asList(args));
	}

	@Override
	public void prepare(int numArgs) {
		//TODO: Add support for init function in udf's
		if (args.length != numArgs)
    {
      args = new Expr[numArgs];
    }
	}

	@Override
	protected void setArgument(int pos, JsonValue value) {
		args[pos] = new ConstExpr(value);
	}

	@Override
	protected void setArgument(int pos, JsonIterator it) {
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
	protected void setArgument(int pos, Expr expr) {
		args[pos] = expr;
	}

	@Override
	protected void setDefault(int pos) {
		throw new UnsupportedOperationException();
	}
	
	public String getClassName() {
		return c.getName();
	}
	
  public String formatError(String msg)
  {
    return "In call of Java UDF " + c.getName() + ": " + msg;
  }
}
