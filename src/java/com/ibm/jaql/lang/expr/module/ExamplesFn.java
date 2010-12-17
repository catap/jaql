package com.ibm.jaql.lang.expr.module;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.core.Namespace;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.EchoedReader;

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
	protected JsonValue evalRaw(Context context) throws Exception {
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
	
	public void executeTest(File f) throws FileNotFoundException 
	{
	  FileInputStream fis = new FileInputStream(f);
	  try {
        Writer writer = new OutputStreamWriter(System.out, "UTF-8");
	    Reader reader = new EchoedReader(new InputStreamReader(fis, "UTF-8"), writer);
	    Jaql.run(f.getName(), reader);
	  } catch (Exception e) {
	    e.printStackTrace();
	  }
	}
}
