package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;

public class ModuleFn extends FileFn {

	public ModuleFn(Expr[] exprs) {
		super(exprs);
	}
	
	@Override
	public Expr location() {
		if(exprs.length == 2) {
			Expr loc = new ConstExpr(new JsonString("ab"));
		} else {
			
		}
		
		return parent;
	}
	
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#isMapReducible()
   */
  @Override
  public boolean isMapReducible()
  {
    return true;
  }
  
  @Override
  public boolean needsModuleInformation() {
  	return true;
  }
}
