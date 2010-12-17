package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

public class ExpandFDExpr extends Expr {
	
	protected static JsonString type = new JsonString("type");
	
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
	  {
	    public Descriptor()
	    {
	      super("expandFD", ExpandFDExpr.class);
	    }
	  }
	
	public ExpandFDExpr(Expr[] exprs){
		super(exprs);
	}

	@Override
	protected JsonValue evalRaw(Context context) throws Exception {				
		OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(exprs[0].eval(context));
		return adapter.expand();			
	}
}
