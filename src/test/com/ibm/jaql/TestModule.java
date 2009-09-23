package com.ibm.jaql;

import java.io.File;
import java.io.IOException;

import com.ibm.jaql.lang.core.Module;

public class TestModule extends JaqlBaseTestCase {

	@Override
	protected void setUp() throws IOException {
		setFilePrefix("module");
		Module.setSearchPath(new String[] {"build/src/test/com/ibm/jaql/modules"});
		System.out.println(new File(".").getAbsolutePath());
		System.out.println(new File("build/src/test/com/ibm/jaql/modules").getAbsolutePath());
	}

	@Override
	protected void tearDown() throws IOException {
		// TODO Auto-generated method stub

	}

}
