package com.ibm.jaql.catalog;

import org.junit.Test;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

// rename to TestCatalog and include it in ant test.
public class CatalogTest {

	@Test
	public void embedded() {
		access(EmbeddedCatalog.init());
	}

	@Test
	public void serverMode() {
		access(new ServerModeCatalog());
	}

	private void access(Catalog testCat) {
		try {
			testCat.open();

			JsonString key = new JsonString("/a/b/c");
			JsonParser jparser = new JsonParser();
			JsonValue val = jparser.parse("{'title': 'Hello', 'comment': 'World'}");

			System.out.println(val.toString());

			testCat.insert(key, val, true);

			System.out.println("Inserted");

			key = new JsonString("/a/x/y");
			val = jparser.parse("{'title': 'Hello2', 'comment': 'World2'}");

			System.out.println(val.toString());

			testCat.insert(key, val, true);

			System.out.println("Inserted");

			JsonArray list = testCat.list(new JsonString(""));
			System.out.println(list.toString());

			System.out.println("============ TEST GETTERS ==============");

			key = new JsonString("/a/b/c");
			System.out.println(testCat.get(key).toString());

			JsonString field1 = new JsonString("title");
			System.out.println(testCat.get(key, field1).toString());

			JsonString field2 = new JsonString("comment");
			BufferedJsonArray fields = new BufferedJsonArray(2);
			fields.set(0, field1);
			fields.set(1, field2);
			System.out.println(testCat.get(key, fields).toString());

			System.out.println("============ TEST UPDATE ==============");

			val = jparser.parse("{'title': 'Good bye', 'new': 'Cruel'}");

			System.out.println("Expected error - cannot overwrite title field.");
			try {
				testCat.update(key, val, false);
			} catch (CatalogException ex) {
				System.out.println(ex.getMessage());
			}
			testCat.update(key, val, true);
			System.out.println(testCat.get(key).toString());

			System.out.println("============ TEST DELETE ==============");

			testCat.delete(new JsonString("/a/b/c"));

			list = testCat.list(new JsonString(""));
			System.out.println(list.toString());

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			testCat.close();
		}
		System.out.println("all done here");
	}
}
