package com.ibm.jaql.lang.expr.module;

import java.util.HashSet;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.NamespaceEnv;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

public class ListExportsFn extends Expr {
	
	public ListExportsFn(Expr[] exprs) {
		super(exprs);
	}

	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11 {
		public Descriptor() {
			super("listExports", ListExportsFn.class);
		}
	}
	
	@Override
	public JsonValue eval(Context context) throws Exception {
		String module = ((JsonString)exprs[0].eval(context)).toString();
		NamespaceEnv namespace = NamespaceEnv.getNamespace(module);
		HashSet<String> exports = namespace.getExports();
		
		BufferedJsonArray arr = new BufferedJsonArray();
		for (String name : exports) {
			arr.add(new JsonString(name));
		}
		
		return arr;
	}

}
