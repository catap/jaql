package com.ibm.jaql;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;

public class TestGlobalVar extends TestCase {
	
	private static String publishers = "[{name: 'Scholastic', country: 'USA'}, "
		+ "{name: 'Grosset', country: 'UK'}, "
		+ "{name: 'Writers Publishing House', country: 'China'}]";

	public void testSetVar() throws Exception {
		Jaql jaql1 = new Jaql(publishers + " -> filter $.country==$cc;");
		Jaql jaql2 = new Jaql(publishers + " -> filter $.country=='UK';");
		jaql1.setVar("$cc", new JsonString("UK"));
		JsonValue jv1 = jaql1.evalNext();
		JsonValue jv2 = jaql2.evalNext();
		Assert.assertEquals(jv1, jv2);
	}

}
