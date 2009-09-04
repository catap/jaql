package com.ibm.jaql.lang.expr.module;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.core.Namespace;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.TeeInputStream;

public class ExamplesFn extends Expr {
	
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12 {
		public Descriptor() {
			super("examples", ExamplesFn.class);
		}
	}

	public ExamplesFn(Expr[] exprs) {
		super(exprs);
	}
	

	@Override
	public JsonValue eval(Context context) throws Exception {
		String name = ((JsonString)exprs[0].eval(context)).toString();
		Namespace namespace = Namespace.get(name);
		Module module = namespace.getModule();
		File[] examples = module.getExampleFiles();
		
		//TODO: Slow down time
		if(exprs[1] == null) {
			String exampleName = ((JsonString)exprs[1].eval(context)).toString();
			for (int i = 0; i < examples.length; i++) {
				if(examples[i].getName().equals(exampleName)) {
					executeTest(examples[i]);
					break;
				}
			}
		} else {
			for (int i = 0; i < examples.length; i++) {
				executeTest(examples[i]);
			}
		}
		
		return null;
	}
	
	public void executeTest(File f) throws FileNotFoundException {
		InputStream in = new FileInputStream(f);
		TeeInputStream tin = new TeeInputStream(in, System.out);
		try {
			Jaql.run(f.getName(), tin);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
