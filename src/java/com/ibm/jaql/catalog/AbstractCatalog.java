package com.ibm.jaql.catalog;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
/**
 * A skeleton implementation of catalog.
 */
public abstract class AbstractCatalog extends Catalog {
	/**
	 * Embedded JDBC connection
	 */
	protected Connection conn;
	/**
	 * JDBC connection url
	 */
	private String url; 
	
	/**
	 * Constructs a catalog with the given JDBC connection url.
	 * 
	 * @param url JDBC connection url
	 */
	public AbstractCatalog(String url) {
		this.url = url;
	}
	
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
				for (JsonValue name : fieldNames) {
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
	
	@Override
	public void open() throws CatalogException
	{
	
		try{
		   conn = DriverManager.getConnection(url);

		   String ddl = "create table catalog ( c_key varchar(1000) primary key, c_entry clob)";
		   
		   Statement st = conn.createStatement();
		   st.executeUpdate(ddl);
		   
		} catch (Exception ex) {
			// Let's ignore errors for now
			//ex.printStackTrace();
		}

	}
	
	@Override
	public void close()
	{
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			// Let's ignore errors for now
			//ex.printStackTrace();
		}
	}
}
