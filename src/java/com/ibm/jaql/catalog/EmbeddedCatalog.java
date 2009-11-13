/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ibm.jaql.catalog;

import com.ibm.jaql.json.type.*;
import com.ibm.jaql.json.parser.JsonParser;
import java.sql.*;


public class EmbeddedCatalog extends Catalog {

	/**
	 * Embedded Derby JDBC connection
	 */
	private Connection conn = null;
	
	@Override
	public void delete( JsonString key) throws CatalogException
	{
		if (conn == null)
			throw new CatalogException("Cannot delete from catalog - connection is not open.");
		
		try {
			Statement st = conn.createStatement();
			st.executeUpdate("delete from catalog where c_key = '" + key.toString() + "'");
		}
		catch (Exception ex) {
			throw new CatalogException("Cannot delete catalog entry with key: "
					+ key.toString() , ex) ;
		}

	}

	@Override
	public JsonValue get( JsonString key) throws CatalogException
	{

		if (conn == null)
			throw new CatalogException("Cannot read from catalog - connection is not open.");
		
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(
						"select c_entry from catalog where c_key = '" + key.toString() + "'");
			if (rs.next()) {
				Clob v = rs.getClob(1);
				JsonParser jparser = new JsonParser(v.getCharacterStream());
				//JsonParser jparser = new JsonParser();
				JsonValue jv = jparser.TopVal();
				return jv;
			}
		}
		catch (Exception ex) {
			throw new CatalogException("Cannot read catalog entry with key: " 
					+ key.toString(), ex) ;
		}
		
		return null;
	}

	@Override
	public JsonValue get( JsonString key, JsonString fieldName) throws CatalogException
	{
		JsonValue jv = this.get(key);
		
		if (jv instanceof JsonRecord) {
			return ((JsonRecord) jv).get(fieldName); 
		}
		
		// TODO should we return error if catalog entry is not a record?
		
		return null;
	}

	@Override
	public JsonValue get( JsonString key, JsonArray fieldNames) throws CatalogException
	{
		JsonValue jv = this.get(key);
		
		try {
			if (jv instanceof JsonRecord) {
				BufferedJsonRecord result = new BufferedJsonRecord();
				for (JsonValue name : fieldNames.iter()) {
					if (name instanceof JsonString) {
						result.add((JsonString) name, ((JsonRecord) jv).get((JsonString) name));
					}
					else {
						String msg = "Field name is not JsonString";
						if (name != null) msg += ": " + name.toString();
						throw new CatalogException(msg);
					}
				}
				return result;
			}
		}
		catch (Exception ex) {
			throw new CatalogException("Cannot read catalog entry with key: " 
					+ key.toString(), ex) ;		
		}
		return null;
	}

	@Override
	public void insert( JsonString key, JsonValue entry, boolean overwrite) 
	throws CatalogException
	{
		if (conn == null)
			throw new CatalogException("Cannot insert into catalog - connection is not open.");
		
		try {
			Statement st = conn.createStatement();
			if (overwrite) {
				st.executeUpdate("delete from catalog where c_key = '" + key.toString() + "'");
			}
			PreparedStatement pst = conn.prepareStatement(
					"insert into catalog (c_key, c_entry) values (?, ?)");
			pst.clearParameters();
			pst.setString(1, key.toString());
			pst.setString(2, entry.toString());
			
			pst.executeUpdate();
		}
		catch (Exception ex) {
			throw new CatalogException("Cannot insert catalog entry with key: " 
					+ key.toString(), ex) ;
		}

	}

	@Override
	public JsonArray list( JsonString keyPrefix) throws CatalogException
	{
		if (conn == null)
			throw new CatalogException("Cannot read from catalog - connection is not open.");
		
		BufferedJsonArray result = new BufferedJsonArray();
		
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(
						"select c_key from catalog where c_key LIKE '%" + 
						keyPrefix.toString() +"'");
			while (rs.next()) {
				JsonValue jv = new JsonString(rs.getString(1));
				result.add(jv);
			}
			return result;
		}
		catch (Exception ex) {
			throw new CatalogException("Cannot list catalog keys starting with " 
					+ keyPrefix.toString(), ex) ;
		}
	}

	@Override
	public void open() throws CatalogException
	{
	
		try{
		   conn = DriverManager.getConnection("jdbc:derby:catalog;create=true");

		   String ddl = "create table catalog ( c_key varchar(1000) primary key, c_entry clob)";
		   
		   Statement st = conn.createStatement();
		   st.executeUpdate(ddl);
		   
		} catch (Exception ex) {
			// Let's ignore errors for now
			//ex.printStackTrace();
		}

	}

	@Override
	public void update( JsonString key, JsonValue entry, boolean overwrite) 
	throws CatalogException
	{
		JsonValue jv = this.get(key);

		// If this "key" does not exist in the catalog, 
		// just insert it - no further checks needed
		if (jv == null) {
			this.insert(key, entry, false);
			return;
		}

		// Otherwise, we need to merge the old and new records
		// Again, assume that jv and entry are JsonRecords 
		// (why not make them records in the API?)

		//		try {
		JsonRecord oldRec = (JsonRecord) jv;
		JsonRecord newRec = (JsonRecord) entry;

		BufferedJsonRecord result = new BufferedJsonRecord();

		for (JsonString name: newRec.asMap().keySet()) {
			result.add(name, newRec.get(name));
		}

		for (JsonString name: oldRec.asMap().keySet()) {
			if (!result.containsKey(name)) {
				result.add(name, oldRec.get(name));
			}
			else {
				if (!overwrite) {
					String msg = "Cannot update catalog entry with key: " 
						+ key.toString() 
						+ ". Cannot overwrite an existing field";
					if (name != null) msg += ": " + name.toString();
					throw new CatalogException(msg);
				}
			}
		}

		this.insert(key, result, true);

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{

		Catalog testCat = Catalog.init();
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
			System.out.println( testCat.get(key).toString() );
			
			JsonString field1 = new JsonString("title");
			System.out.println( testCat.get(key, field1).toString() );
			
			JsonString field2 = new JsonString("comment");
			BufferedJsonArray fields = new BufferedJsonArray(2);
			fields.set(0,field1);
			fields.set(1,field2);
			System.out.println( testCat.get(key,fields).toString() );
			
			System.out.println("============ TEST UPDATE ==============");
			
			val = jparser.parse("{'title': 'Good bye', 'new': 'Cruel'}");
			
			System.out.println("Expected error - cannot overwrite title field.");
			try {
				testCat.update(key, val, false);
			} catch (CatalogException ex) {
				System.out.println(ex.getMessage());
			}
			testCat.update(key, val, true);
			System.out.println( testCat.get(key).toString() );
			
			System.out.println("============ TEST DELETE ==============");
			
			testCat.delete(new JsonString("/a/b/c"));
			
			list = testCat.list(new JsonString(""));
			System.out.println(list.toString());			
			
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("all done here");

	}

}
