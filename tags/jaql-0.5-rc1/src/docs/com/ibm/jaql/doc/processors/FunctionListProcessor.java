package com.ibm.jaql.doc.processors;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;

import com.ibm.jaql.doc.UDFDesc;

public class FunctionListProcessor implements DataProcessor {
	String sourcePath;
	final String className = "JaqlFnList";
	final String packageName = "com.ibm.jaql.lang.core";

	@Override
	public void process(List<UDFDesc> list, HashMap<String, String> options) {
		sourcePath = options.get("-srcpath");

		StringBuilder addStatements = new StringBuilder();
		for (UDFDesc udfDesc : list) {
			addStatements.append("list.add(" + udfDesc.getFullyQualifiedClassName()
					+ ".class);\n");
		}

		// Open outputfile
		File f = openOutputFile();
		PrintStream srcWriter = null;
		try {
			srcWriter = new PrintStream(f);
		} catch (IOException e) {
			throw new RuntimeException(FunctionListProcessor.class
					+ " could not create function list class file", e);
		}

		// Generate source for class
		srcWriter
				.print("package "
						+ packageName
						+ ";\n"
						+ "import java.util.ArrayList;\n"
						+ "import com.ibm.jaql.lang.expr.core.JaqlFn;\n"
						+ "import com.ibm.jaql.lang.expr.core.Expr;\n"
						+ "public class "
						+ className
						+ " implements com.ibm.jaql.lang.core.FunctionList {\n"
						+ "static ArrayList<Class<? extends Expr>> list = new ArrayList<Class<? extends Expr>>();\n"
						+ "static {\n"
						+ addStatements.toString()
						+ "}"
						+ "public ArrayList<Class<? extends Expr>> getList() {\n"
						+ "	return list;\n" + "}\n" + "}\n");
		srcWriter.close();
	}

	File openOutputFile() {
		String filePath = sourcePath + File.separator
				+ packageName.replace(".", File.separator) + File.separator
				+ className + ".java";
		File f = new File(filePath);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!f.canWrite()) {
			throw new RuntimeException("Cannot write to file " + filePath);
		}
		return f;
	}

}
