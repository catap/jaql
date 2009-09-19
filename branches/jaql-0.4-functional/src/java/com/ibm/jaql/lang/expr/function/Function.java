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

/** Superclass for JSON values that represent a function definition.
 * 
 * Implementation provide functionality to call and inline the represented function. To that extent,
 * they provide various <code>setArguments</code> methods as well as {@link #eval(Context)} and
 * {@link #inline(boolean)}. Arguments are transient; they are not considered part of the 
 * function itself.    
 */
public abstract class Function extends JsonAtom
{
  /** decompiled code of the function definition */  
  private String text;

  
  // -- self-description --------------------------------------------------------------------------
  
  /** Returns information about the formal parameters of this function. */
  public abstract Parameters<?> getParameters();
  
  /** Produces an error string including the specified message and some information about this
   * function. */
  protected abstract String formatError(String msg);
  
  /** Returns a text representation of this function. */
  public String getText()
  {
    if (text == null)
    {
      try
      {
        this.text = JsonUtil.printToString(this);
      } catch (IOException e)
      {
        throw JaqlUtil.rethrow(e);
      }
    }
    return text;
  }
  
  
  // -- evaluation / inlining ---------------------------------------------------------------------

  /** Prepare the data structure used by the <code>setArgument</code> and <code>setDefault</code>
   * methods. This method will always be called before any calls to <code>setArgument</code> or 
   * <code>setDefault</code> and arguments <code>0,1,...,noArgs-1</code> will be set 
   * via calls to those functions afterwards. The value of <code>noArgs</code>num is guaranteed to 
   * be large enough to assign a value to all of the functions parameters, as determined by 
   * {@link #getNumPassedArguments(int)}. 
   */
  protected abstract void prepare(int noArgs);
  
  /** Set the i-th positional argument to the specified value. */
  protected abstract void setArgument(int pos, JsonValue value);
  
  /** Set the i-th positional argument to an array containing the values produced by the specified 
   * iterator. */
  protected abstract void setArgument(int pos, JsonIterator it);

  /** Set the i-th positional argument to the specified expression. */
  protected abstract void setArgument(int pos, Expr expr);
  
  /** Set the i-th positional argument to its default value. */
  protected abstract void setDefault(int pos);
  
  /** Inlines this function and its arguments. The arguments have to be set using one of the 
   * <code>setArguments</code> functions. 
   * 
   * Inlining requires modification of some of the argument expressions. When inlining is performed
   * in order to integrate this function into Jaql's AST, these modifications are allowed; 
   * otherwise the function's arguments might have to be cloned in order to leave their expressions
   * intact. The <code>forEval</code> argument distinguishes between those cases. The general
   * contract is that the call sequence is <code>inline(true)* inline(false)</code> for each
   * set of arguments.
   */
  public abstract Expr inline(boolean forEval);
  
  /** Evaluate this function. The arguments have to be set using one of the 
   * <code>setArguments</code> functions. */
  public JsonValue eval(Context context) throws Exception
  {
    Expr f = inline(true); 
    return f.eval(context);
  }

  /** Evaluate this function. The arguments have to be set using one of the 
   * <code>setArguments</code> functions. */
  public JsonIterator iter(Context context) throws Exception
  {
    Expr f = inline(true);
    return f.iter(context);
  }
  

  // -- arguments processing ---------------------------------------------------------------------- 

  /** Returns <code>true</code> if this function can be called with <code>numPositionalArgs</code> 
   * positional arguments. */
  public boolean canBeCalledWith(int numPositionalArgs)
  {
    Parameters<?> pars = getParameters();
    if (( pars.numParameters() < numPositionalArgs && !pars.hasRepeatingParameter() ) || pars.numRequiredParameters() > numPositionalArgs)
    {
      return false;
    }
    return true;
  }
  
  /** Returns the number of all arguments, including default arguments,
   * that are passed to the this function if the function call itself contains <code>numArgs</code> 
   * arguments. */
  public int getNumPassedArguments(int numArgs)
  {
    Parameters<?> pars = getParameters();
    return Math.max(numArgs, pars.hasRepeatingParameter() 
        ? pars.numParameters()-1 : pars.numParameters());
  }
  
  /** Sets the arguments for evaluation/inlining using either positional-only, or positional 
   * and named argument expressions. The expressions are not evaluated.
   * 
   * The arguments are taken from the <code>args</code> array, from positions <code>start</code> 
   * to <code>start+length-1</code>. 
   * 
   * If <code>named</code> is <code>true</code>, <code>args</code> has form 
   * (name, value, name, value, ...). In this case,
   * a name that evaluates to a non-null value corresponds to a named argument, a name that
   * evaluates to <code>null</code> corresponds to a positional argument. If <code>name</code>
   * is <code>false</code>, <code>args</code> has from (value, value, ...).
   */
  public void setArguments(Expr[] args, int start, int length, boolean named)
  {
    if (named)
    {
      int n = getNumPassedArguments(length/2);
      prepare(n);
      prepareNamed(args, start, length);
    }
    else
    {
      int n = getNumPassedArguments(length);
      prepare(n);
      preparePositional(args, start, length);
    }
  }

  /** Sets the arguments for evaluation/inlining to the specified positional arguments. Shortcut for 
   * <code>setArguments(args, 0, args.length, false)</code>. */
  public void setArguments(Expr ... args) throws Exception
  {
    setArguments(args, 0, args.length, false);
  }


  /** Sets the arguments for evaluation/inlining the specified positional arguments. */
  public void setArguments(JsonValue[] args, int start, int length)
  {
    if (!canBeCalledWith(length))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = getNumPassedArguments(length);
    prepare(n);
    for (int i=0; i<args.length; i++)
    {
      setArgument(i, args[i]);
    }
  }
  
  /** Sets the arguments for evaluation/inlining the specified positional arguments. Shortcut for 
   * <code>setArguments(args, 0, args.length)</code>. */
  public void setArguments(JsonValue ... args)
  {
    setArguments(args, 0, args.length);
  }
  
  /** Sets the argument for evaluation/inlining.
   * 
   * @param arg0 iterator; function argument is array of values produced by it
   */
  public void setArguments(JsonIterator arg0)
  {
    if (!canBeCalledWith(1))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = getNumPassedArguments(1);
    prepare(n);
    setArgument(0, arg0);
    for (int i=1; i<n; i++)
    {
      setDefault(i);
    }
  }

  /** Sets the arguments for evaluation/inlining.
   * 
   * @param arg0 iterator; function argument is array of values produced by it
   * @param arg1 argument
   */
  public void setArguments(JsonIterator arg0, JsonValue arg1)
  {
    if (!canBeCalledWith(2))
    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = getNumPassedArguments(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }
  
  /** Sets the arguments for evaluation/inlining.
   * 
   * @param arg0 iterator; function argument is array of values produced by it
   * @param arg1 iterator; function argument is array of values produced by it
   */
  public void setArguments(JsonIterator arg0, JsonIterator arg1)
  {
    if (!canBeCalledWith(2))    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = getNumPassedArguments(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }

  /** Sets the arguments for evaluation/inlining.
   * 
   * @param arg0 argument
   * @param arg1 iterator; function argument is array of values produced by it
   */
  public void setArguments(JsonValue arg0, JsonIterator arg1)
  {
    if (!canBeCalledWith(2))    {
      throw new IllegalArgumentException(formatError("invalid number of arguments provided"));
    }
    int n = getNumPassedArguments(2);
    prepare(n);
    setArgument(0, arg0);
    setArgument(1, arg1);
    for (int i=2; i<n; i++)
    {
      setDefault(i);
    }
  }
  
  /** Prepares this function for evaluation/inlining for the specified positional arguments. */
  private void preparePositional(Expr[] args, int argsStart, int numArgs)
  {
    Parameters<?> pars = getParameters();
    int numParams = pars.numParameters();
    boolean hasRepeatingParam = pars.hasRepeatingParameter();
    boolean hasRepeatingArgs = hasRepeatingParam && numArgs >= numParams;
    int n = getNumPassedArguments(numArgs); 
    
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
  
  /** Prepares this function for evaluation/inlining for the specified named arguments. */
  protected void prepareNamed(Expr[] args, int argsStart, int argsLength)
  {
    // initialize
    Parameters<?> pars = getParameters();
    assert argsLength % 2 == 0;
    int numArgs = argsLength / 2;
    int numParams = pars.numParameters();
    boolean hasRepeatingParam = pars.hasRepeatingParameter();
    boolean hasRepeatingArgs = hasRepeatingParam && numArgs >= numParams;
    int n = getNumPassedArguments(numArgs); 
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
  
  // -- comparison / hashing / copying ------------------------------------------------------------ 

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

  // fixes return type
  @Override
  public abstract Function getCopy(JsonValue target);

  // changes return type
  @Override
  public abstract Function getImmutableCopy();
}
