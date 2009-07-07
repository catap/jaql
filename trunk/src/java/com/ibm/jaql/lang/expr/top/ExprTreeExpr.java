package com.ibm.jaql.lang.expr.top;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/** An internal method that can be used to print the internal tree of expressions in JSON format. */
@JaqlFn(fnName = "exprtree", minArgs = 1, maxArgs = 1)
public class ExprTreeExpr extends TopExpr
{
  /**
   * boolean explain expr
   * 
   * @param exprs
   */
  public ExprTreeExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public ExprTreeExpr(Expr expr)
  {
    super(new Expr[] {expr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("explain ");
    exprs[0].decompile(exprText, capturedVars);
  }

  public Map<ExprProperty, Boolean> getProperties() {
    return ExprProperty.createSafeDefaults(); // don;t optimize in any way
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonRecord eval(Context context) throws Exception
  {
    return constructRecord(this);
  }
  
  // TODO: assumes no cycles -- always valid?
  private JsonRecord constructRecord(Expr expr)
  {
    BufferedJsonRecord r = new BufferedJsonRecord();
    r.set(new JsonString("A-classname"), new JsonString(expr.getClass().getCanonicalName()));
    
    r.set(new JsonString("B-schema"), new JsonSchema(expr.getSchema()));
    
    int i = 1;
    for (Expr e : expr.children()) {
      r.set(new JsonString("C-child" + i), constructRecord(e));
      i++;
    }
    
    return r;
  }
}
