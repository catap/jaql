package com.ibm.jaql.lang.expr.core;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JsonComparator;

public abstract class CmpExpr extends Expr
{
  public CmpExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public CmpExpr(ArrayList<? extends Expr> exprs)
  {
    super(exprs);
  }

  public CmpExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  public CmpExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  public CmpExpr(Expr expr0)
  {
    super(expr0);
  }

  /**
   * The comparator to use on the result of eval()
   * 
   * @param context
   * @return
   */
  public abstract JsonComparator getComparator(Context context);
}
