package com.ibm.jaql.lang.expr.function;

import java.io.IOException;
import java.util.BitSet;

import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

public abstract class Function extends JsonAtom
{
  private String fnText;

  @Override
  public int compareTo(Object o)
  {
    throw new IllegalStateException(formatError("functions cannot be compared"));
  }

  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.FUNCTION;
  }

  @Override
  public long longHashCode()
  {
    throw new IllegalStateException(formatError("functions cannot be hashed"));
  }
  
  public abstract Function getCopy(JsonValue target);

  public abstract Function getImmutableCopy();
  
  // -- self-description --------------------------------------------------------------------------
  
  public abstract Parameters<?> getParameters();
  
  public abstract String formatError(String message);
  
  public String getText()
  {
    if (fnText == null)
    {
      try
      {
        this.fnText = JsonUtil.printToString(this);
      } catch (IOException e)
      {
        throw JaqlUtil.rethrow(e);
      }
    }
    return fnText;
  }
  
  // -- evaluation --------------------------------------------------------------------------------

  public abstract Expr inline(boolean eval);

  public abstract void prepare(int numArgs);
  
  public JsonValue eval(Context context) throws Exception
  {
    Expr f = inline(true); 
    return f.eval(context);
  }

  public JsonIterator iter(Context context) throws Exception
  {
    Expr f = inline(true);
    return f.iter(context);
  }
  
  protected abstract void setArgument(int pos, JsonValue value);
  
  protected abstract void setArgument(int pos, JsonIterator it);
  
  protected abstract void setArgument(int pos, Expr expr);
  
  protected abstract void setDefault(int pos);
  
  public boolean canBeCalledWith(int noArgs)
  {
    Parameters<?> pars = getParameters();
    if (pars.noParameters() < noArgs|| pars.noRequiredParameters() > noArgs)
    {
      return false;
    }
    return true;
  }
  
  // -- arguments processing ---------------------------------------------------------------------- 
  
  public int numPositionalArgs(int numArgs)
  {
    Parameters<?> pars = getParameters();
    return Math.max(numArgs, pars.hasRepeatingParameter() 
        ? pars.noParameters()-1 : pars.noParameters());
  }
  
  public void setArguments(Expr ... args) throws Exception
  {
    setArguments(args, 0, args.length, false);
  }
  
  public void setArguments(Expr[] args, int start, int length, boolean named)
  {
    if (named)
    {
      int n = numPositionalArgs(length/2);
      prepare(n);
      initializeNamed(args, start, length);
    }
    else
    {
      int n = numPositionalArgs(length);
      prepare(n);
      initializePositional(args, start, length);
    }
  }

  public void setArguments(JsonValue ... args)
  {
    setArguments(args, 0, args.length);
  }
  
  public void setArguments(JsonValue[] args, int start, int length)
  {
    if (!canBeCalledWith(length))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = numPositionalArgs(length);
    prepare(n);
    for (int i=0; i<args.length; i++)
    {
      setArgument(i, args[i]);
    }
  }

  public void setArguments(JsonIterator arg0)
  {
    if (!canBeCalledWith(1))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = numPositionalArgs(1);
    prepare(n);
    setArgument(0, arg0);
    for (int i=1; i<n; i++)
    {
      setDefault(i);
    }
  }

  public void setArguments(JsonIterator arg0, JsonValue arg1)
  {
    if (!canBeCalledWith(2))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = numPositionalArgs(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }
  
  public void setArguments(JsonIterator arg0, JsonIterator arg1)
  {
    if (!canBeCalledWith(2))    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = numPositionalArgs(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }

  public void setArguments(JsonValue arg0, JsonIterator arg1)
  {
    if (!canBeCalledWith(2))    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = numPositionalArgs(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }
  
  protected void initializePositional(Expr[] args, int argsStart, int numArgs)
  {
    Parameters<?> pars = getParameters();
    int numParams = pars.noParameters();
    boolean hasRepeatingParam = pars.hasRepeatingParameter();
    boolean hasRepeatingArgs = hasRepeatingParam && numArgs >= numParams;
    int n = numPositionalArgs(numArgs); 
    
    // check if there are too many arguments
    if (!hasRepeatingArgs && n>numParams)
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    
    // copy over all arguments
    int i=0;
    for (; i<numArgs; i++)
    {
      setArgument(i, args[argsStart+i]);
    }
    
    // set default values
    for (; i<n; i++)
    {
      if (!pars.hasDefault(i))
      {
        throw new IllegalArgumentException(formatError("missing value for parameter " + pars.nameOf(i))); 
      }
      setDefault(i);
    }
  }
  
  // fills positions from resultStart to resultStart+numPositionalArgs exc;isove
  protected void initializeNamed(Expr[] args, int argsStart, int argsLength)
  {
    // initialize
    Parameters<?> pars = getParameters();
    assert argsLength % 2 == 0;
    int numArgs = argsLength / 2;
    int numParams = pars.noParameters();
    boolean hasRepeatingParam = pars.hasRepeatingParameter();
    boolean hasRepeatingArgs = hasRepeatingParam && numArgs >= numParams;
    int n = numPositionalArgs(numArgs); 
    BitSet initializedArgs = new BitSet(n);

    // check if there are too many arguments
    if (!hasRepeatingArgs && n>numParams)
    {
      throw new IllegalArgumentException(formatError("too many arguments provided"));
    }
    
    // copy over all arguments
    boolean hasNamed = false;
    for (int i=0; i<numArgs; i++)
    {
      Expr nameExpr = args[argsStart+2*i];
      JsonString name = null;
      try
      {
        assert nameExpr.isCompileTimeComputable().always(); // it's actually a ConstExpr(JsonString) or ConstExpr(null)
        name = (JsonString)nameExpr.eval(Env.getCompileTimeContext());
      } catch (Exception e)
      {
        throw JaqlUtil.rethrow(e);
      }
      if (name == null)
      {
        if (hasNamed) 
        {
          throw new IllegalArgumentException(formatError("found positional argument after named argument"));
        }
        setArgument(i, args[argsStart+2*i+1]);
        initializedArgs.set(i);
      }
      else
      {
        if (hasRepeatingArgs)
        {
          throw new IllegalArgumentException(formatError("named arguments disallowed when repeating parameter"
              + "is used"));
        }
        int p = pars.positionOf(name);
        if (p<0) throw new IllegalArgumentException(formatError("invalid argument: " + name));
        if (initializedArgs.get(p))
        {
          throw new IllegalArgumentException(formatError("duplicate argument: " + name));
        }
        setArgument(p, args[argsStart+2*i+1]);
        initializedArgs.set(p);
        hasNamed = true;
      }
    }
    
    // set default values
    for (int i=initializedArgs.nextClearBit(0); i<n; i=initializedArgs.nextClearBit(i+1))
    {
      if (!pars.hasDefault(i))
      {
        throw new IllegalArgumentException(formatError("missing value for parameter: " + pars.nameOf(i)));
      }
      setDefault(i);
    }
  }
}
