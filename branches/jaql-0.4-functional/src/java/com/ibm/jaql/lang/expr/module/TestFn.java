package com.ibm.jaql.lang.expr.module;
//
//import java.io.File;
//import java.io.IOException;
//
//import com.ibm.jaql.JaqlBaseTestCase;
//import com.ibm.jaql.json.type.JsonString;
//import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.core.Context;
//import com.ibm.jaql.lang.core.Module;
//import com.ibm.jaql.lang.core.NamespaceEnv;
//import com.ibm.jaql.lang.expr.core.Expr;
//import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
//
//public class TestFn extends Expr {
//	
//	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12 {
//		public Descriptor() {
//			super("test", TestFn.class);
//		}
//	}
//	
//	public TestFn(Expr[] exprs) {
//		super(exprs);
//	}
//
//	@Override
//	public JsonValue eval(Context context) throws Exception {
//		String name = ((JsonString)exprs[0].eval(context)).toString();
//		NamespaceEnv namespace = NamespaceEnv.getNamespace(name);
//		Module module = namespace.getModule();
//		String[] tests = module.getTests();
//		
//		ModuleTestCase testCase = null;
//		
//		if(exprs[1] == null) {
//			String testName = ((JsonString)exprs[1].eval(context)).toString();
//			for (int i = 0; i < tests.length; i++) {
//				if(tests[i].equals(testName)) {
//					testCase = new ModuleTestCase(module.getTestDirectory(), testName);
//					testCase.setUp();
//					testCase.testQueries();
//					testCase.tearDown();
//					break;
//				}
//			}
//		} else {
//			for (int i = 0; i < tests.length; i++) {
//				testCase = new ModuleTestCase(module.getTestDirectory(), tests[i]);
//				testCase.setUp();
//				testCase.testQueries();
//				testCase.tearDown();
//			}
//		}
//		
//		return null;
//	}
//
//}
//
//class ModuleTestCase extends JaqlBaseTestCase {
//	File dir;
//	String name;
//	
//	ModuleTestCase(File directory, String name) {
//		dir = directory;
//		this.name = name;
//	}
//
//	@Override
//	protected void setUp() throws IOException {
//		setFilePrefix(dir, name);
//	}
//
//	@Override
//	protected void tearDown() throws IOException {
//	}
//}