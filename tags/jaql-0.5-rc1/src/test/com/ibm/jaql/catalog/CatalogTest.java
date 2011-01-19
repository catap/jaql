package com.ibm.jaql.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.ibm.jaql.AbstractTest;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonRecord;

// TODO renamed to TestCatalog to be be included in "ant test" when the catalog feature is finished.
public class CatalogTest extends AbstractTest {

	private Catalog cat;

	@Before
	public void cleanup() throws Exception {
		cat = new CatalogImpl();
	}	
	
	@Test
	public void closeWithoutUpdate() throws Exception{		
		cat.open();
		cat.close();
	}	

	@Test
	public void access() throws Exception {
		try {
			//open database
			cat.open();
			((CatalogImpl) cat).cleanup();
			
			// insert
			JsonString key1 = new JsonString("/a/b/c");
			JsonRecord val1 = (JsonRecord) parse("{'title': 'Hello', 'comment': 'World'}");
			cat.insert(key1, val1, false);

			JsonString key2 = new JsonString("/a/x/y");
			JsonRecord val2 = (JsonRecord) parse("{'title': 'Hello2', 'comment': 'World2'}");
			cat.insert(key2, val2, true);

			BufferedJsonArray expectedkeys = new BufferedJsonArray(2);
			expectedkeys.set(0, key1);
			expectedkeys.set(1, key2);
			assertEquals(expectedkeys, cat.list(new JsonString("")));		

			// getters
			assertEquals(val1, cat.get(key1));
			JsonString field1 = new JsonString("title");
			assertEquals(new JsonString("Hello"), cat.get(key1, field1));

			JsonString field2 = new JsonString("comment");
			BufferedJsonArray fields = new BufferedJsonArray(2);
			fields.set(0, field1);
			fields.set(1, field2);
			assertEquals(val1, cat.get(key1, fields));

			// update
			JsonRecord val3 = (JsonRecord) parse("{'title': 'Good bye', 'new': 'Cruel'}");
			try {
				cat.update(key1, val3, false);
				fail("Expected error - cannot overwrite title field.");
			} catch (CatalogException ex) {
				infoException(ex);
			}
			cat.update(key1, val3, true);
			assertEquals(parse("{'title': 'Good bye', 'comment': 'World', 'new': 'Cruel'}"),
			             cat.get(key1));

			// delete
			cat.delete(key1);
			cat.delete(key2);
			assertEquals((long) 0, cat.list(new JsonString("")).count());
		} finally {
			cat.close();
		}
	}	
}