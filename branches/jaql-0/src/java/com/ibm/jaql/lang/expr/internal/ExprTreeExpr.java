package com.ibm.jaql.lang.expr.internal;

import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/** An internal method that can be used to print the internal tree of expressions in JSON format. */
public class ExprTreeExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("exprtree", ExprTreeExpr.class);
    }
  }
  
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
