package com.ibm.jaql.util;

import junit.framework.Assert;

import com.ibm.jaql.AbstractTest;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;

import org.junit.Test;

public class TestClassLoader extends AbstractTest {

	public static final String A_JAR = "build/test/cache/util/A.jar";
	
	public static final String B_JAR = "build/test/cache/util/B.jar";
	
	/**
	 * If there are dependencies between jars, check that
	 * the new class loader produced by the ClassLoaderManager
	 * can resolve them appropriately.
	 * 
	 * @throws Exception
	 */
	@Test
	public void jarDependencies() throws Exception {
		
		// register a function B and invoke it
		String test = "test = javaudf('B'); test();";
		
		// B derives from A. There was a regression where 
		// the ordering below resulted in incorrect class resolution
		Jaql.addExtensionJars(new String[] { B_JAR, A_JAR });
		
		// evaluate the expression
		Jaql jaql = new Jaql();
		jaql.setInput(test);
		JsonValue val = jaql.evalNext();
		
		// we should get something non-null back
		Assert.assertTrue("Expected non-null string", ((JsonString)val).bytesLength() > 0);
	}
}
