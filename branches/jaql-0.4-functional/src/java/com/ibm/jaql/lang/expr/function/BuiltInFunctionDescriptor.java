package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.expr.core.Expr;

/** Descriptor for built-in functions. */
public interface BuiltInFunctionDescriptor
{
  /** Returns the name of the function. */
  public String getName();
  
  /** Returns a description of the formal parameters of the function */
  public JsonValueParameters getParameters();
  
  /** Derives the schema of a result of the function call with the specified arguments.
   * This schema is mainly used for documentation purposes; during compilation, 
   * {@link Expr#getSchema()} will be used. */
  public Schema getSchema();
  
  public Class<? extends Expr> getImplementingClass();
  
  public Expr construct(Expr[] positionalArgs);
}
