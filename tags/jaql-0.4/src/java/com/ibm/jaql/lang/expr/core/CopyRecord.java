package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.Bool3;


public class CopyRecord extends FieldExpr
{

  /**
   * 
   * @param exprs
   */
  public CopyRecord(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param recExpr
   * @param nameExpr
   */
  public CopyRecord(Expr recExpr)
  {
    super(recExpr);
  }

  /**
   * 
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(").*");
  }

  /**
   * 
   */
  @Override
  public void eval(Context context, MemoryJRecord outrec) throws Exception
  {
    JRecord inrec = (JRecord) exprs[0].eval(context).get();
    if (inrec != null)
    {
      int m = inrec.arity();
      for (int j = 0; j < m; j++)
      {
        outrec.add(inrec.getName(j), inrec.getValue(j));
      }
    }
  }

  /**
   * 
   */
  @Override
  public Bool3 staticNameMatches(JString name)
  {
    return Bool3.UNKNOWN;
  }
}
