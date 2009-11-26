package com.ibm.jaql;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.Jaql;

public class TestGlobalVar extends TestCase {

	public void testSetVar() throws Exception {
		Jaql jaql1 = new Jaql("read(hdfs($fName));");
		Jaql jaql2 = new Jaql("read(hdfs('books'));");
		jaql1.setVar("$fName", new JsonString("books"));
		JsonValue jv1 = jaql1.evalNext();
		JsonValue jv2 = jaql2.evalNext();
		Assert.assertEquals(jv1, jv2);

		Jaql jaql3 = new Jaql("read(hdfs('books'))->filter $.year>$minYear;");
		Jaql jaql4 = new Jaql("read(hdfs('books'))->filter $.year>1997;");
		jaql3.setVar("$minYear", new JsonDecimal(1997));
		JsonValue jv3 = jaql3.evalNext();
		JsonValue jv4 = jaql4.evalNext();
		Assert.assertEquals(jv3, jv4);
	}

}
