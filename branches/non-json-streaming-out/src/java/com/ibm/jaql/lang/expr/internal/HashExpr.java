package com.ibm.jaql.lang.expr.internal;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/** An internal method that can be used to print the internal tree of expressions in JSON format. */
@JaqlFn(fnName = "hash", minArgs = 1, maxArgs = 1)
public class HashExpr extends Expr
{
  /**
   * boolean explain expr
   * 
   * @param exprs
   */
  public HashExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public HashExpr(Expr expr)
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
    return new JsonLong(JsonUtil.hashCode(exprs[0].eval(context)));
  }
}
