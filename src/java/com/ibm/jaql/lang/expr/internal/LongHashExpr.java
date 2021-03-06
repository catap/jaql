package com.ibm.jaql.lang.expr.internal;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** An internal method that can be used to print the internal tree of expressions in JSON format. */
public class LongHashExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("longHash", LongHashExpr.class);
    }
  }
  
  /**
   * boolean explain expr
   * 
   * @param exprs
   */
  public LongHashExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public LongHashExpr(Expr expr)
  {
    super(new Expr[] {expr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonLong eval(Context context) throws Exception
  {
    return new JsonLong(JsonUtil.longHashCode(exprs[0].eval(context)));
  }
}
