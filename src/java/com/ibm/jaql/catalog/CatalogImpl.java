package com.ibm.jaql.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * Catalog implementation. By default, embedded derby database is used. With
 * embedded derby database, only one active user is supported. By change <i>
 * ConnectionURL</i> in catalog connection configuration file
 * <i>conf/catalog-conf.jql</i>, server mode derby database can be use to
 * support multiple users. Derby 10.5.3.0 has been tested. So it is suggested to
 * use this version. The following text gives a short description about how to
 * start up derby database in server mode.
 * 
 * <ul>
 * <li>Configure DERBY_HOME and DERBY_INSTALL environment variables.</li>
 * <li>Issue
 * <tt>java -jar $DERBY_HOME/lib/derbyrun.jar server start -h 0.0.0.0</tt> to
 * start derby.</li>
 * <ul>
 */
public class CatalogImpl implements Catalog {

	private static final JsonString CONNECTION_URL = new JsonString("ConnectionURL");
	private static final String CATALOG_CONFIG = "catalog-conf.jql";

	private Logger LOG = Logger.getLogger(getClass());

	/**
	 * Embedded JDBC connection
	 */
	private Connection conn;

	/**
	 * JDBC connection url - defaults to Embedded derby.
	 */
	private String url = "jdbc:derby:catalog;create=true";

	/**
	 * Constructs a catalog connection as specified by the catalog connection
	 * configuration file.
	 */
	public CatalogImpl() throws CatalogException {
		InputStream input = null;
		try {
			input = ClassLoaderMgr.getResourceAsStream(CATALOG_CONFIG);
			JsonParser parser = new JsonParser(input);
			JsonRecord rec = (JsonRecord) parser.TopVal();
			JsonString connUrl = (JsonString) rec.get(CONNECTION_URL);
			if (connUrl == null || connUrl.toString().trim().equals(""))
				throw new NullPointerException(CONNECTION_URL + " is not specified in "
				    + CATALOG_CONFIG);
			url = connUrl.toString();
		} catch (Exception ex) {
			LOG.debug("Cannot get " + CONNECTION_URL + " from "
			    + CATALOG_CONFIG, ex);
			LOG.debug("Defaulting to embedded Derby for catalog storage.");
			url = "jdbc:derby:catalog;create=true";
		} finally {
			closeStream(input);
		}
	}

	@Override
	public void delete(JsonString key) throws CatalogException {
		checkConnection();
		Statement st = null;
		try {
			st = conn.createStatement();
			st.executeUpdate("delete from catalog where c_key = '" + key.toString()
			    + "'");
			conn.commit();
		} catch (Exception ex) {
			rollback();
			throw new CatalogException("Cannot delete catalog entry with key: "
			    + key.toString(), ex);
		} finally {
			closeStatement(st);
		}
	}

	@Override
	public JsonRecord get(JsonString key) throws CatalogException {
		checkConnection();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			rs = st.executeQuery("select c_entry from catalog where c_key = '"
			    + key.toString() + "'");
			if (rs.next()) {
				Clob v = rs.getClob(1);
				JsonParser jparser = new JsonParser(v.getCharacterStream());
				JsonRecord jv = (JsonRecord) jparser.TopVal();
				conn.commit();
				return jv;
			}
			conn.commit();
		} catch (Exception ex) {
			rollback();
			throw new CatalogException("Cannot read catalog entry with key: "
			    + key.toString(), ex);
		} finally {
			closeResultSet(rs);
			closeStatement(st);
		}
		return null;
	}

	@Override
	public JsonValue get(JsonString key, JsonString fieldName) throws CatalogException {
		JsonRecord jv = this.get(key);
		return jv.get(fieldName);
		
	}

	@Override
	public JsonRecord get(JsonString key, JsonArray fieldNames) throws CatalogException {
		JsonRecord jv = this.get(key);
		try {
			if (jv instanceof JsonRecord) {
				BufferedJsonRecord result = new BufferedJsonRecord();
				for (JsonValue name : fieldNames) {
					if (name instanceof JsonString) {
						result.add((JsonString) name,
						           ((JsonRecord) jv).get((JsonString) name));
					} else {
						String msg = "Field name is not JsonString";
						if (name != null)
							msg += ": " + name.toString();
						throw new CatalogException(msg);
					}
				}
				return result;
			}
		} catch (Exception ex) {
			throw new CatalogException("Cannot read catalog entry with key: "
			    + key.toString(), ex);
		}
		return null;
	}

	@Override
	public void insert(JsonString key, JsonRecord entry, boolean overwrite) throws CatalogException {
		checkConnection();
		Statement st = null;
		PreparedStatement pst = null;
		try {
			st = conn.createStatement();
			if (overwrite) {
				st.executeUpdate("delete from catalog where c_key = '" + key.toString()
				    + "'");
			}
			pst = conn.prepareStatement("insert into catalog (c_key, c_entry) values (?, ?)");
			pst.clearParameters();
			pst.setString(1, key.toString());
			pst.setString(2, entry.toString());
			pst.executeUpdate();
			conn.commit();
		} catch (Exception ex) {
			rollback();
			throw new CatalogException("Cannot insert catalog entry with key: "
			    + key.toString(), ex);
		} finally {
			closeStatement(st);
			closeStatement(pst);
		}
	}

	@Override
	public JsonArray list(JsonString keyPrefix) throws CatalogException {
		checkConnection();
		BufferedJsonArray result = new BufferedJsonArray();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			rs = st.executeQuery("select c_key from catalog where c_key LIKE '%"
			    + keyPrefix.toString() + "'");
			while (rs.next()) {
				JsonValue jv = new JsonString(rs.getString(1));
				result.add(jv);
			}
			conn.commit();
			return result;
		} catch (Exception ex) {
			rollback();
			throw new CatalogException("Cannot list catalog keys starting with "
			    + keyPrefix.toString(), ex);
		} finally {
			closeResultSet(rs);
			closeStatement(st);
		}
	}

	@Override
	public void update(JsonString key, JsonRecord entry, boolean overwrite) throws CatalogException {
		JsonRecord jv = this.get(key);

		// If this "key" does not exist in the catalog,
		// just insert it - no further checks needed
		if (jv == null) {
			insert(key, entry, false);
			return;
		}

		// Otherwise, we need to merge the old and new records
		JsonRecord oldRec = (JsonRecord) jv;
		JsonRecord newRec = (JsonRecord) entry;

		BufferedJsonRecord result = new BufferedJsonRecord();

		for (JsonString name : newRec.asMap().keySet()) {
			result.add(name, newRec.get(name));
		}

		for (JsonString name : oldRec.asMap().keySet()) {
			if (!result.containsKey(name)) {
				result.add(name, oldRec.get(name));
			} else {
				if (!overwrite) {
					String msg = "Cannot update catalog entry with key: "
					    + key.toString() + ". Cannot overwrite an existing field";
					if (name != null)
						msg += ": " + name.toString();
					throw new CatalogException(msg);
				}
			}
		}
		insert(key, result, true);
	}

	@Override
	public void open() throws CatalogException {
		try {
			conn = DriverManager.getConnection(url);
			conn.setAutoCommit(false);
		} catch (Exception ex) {
			throw new CatalogException("Cannot open a JDBC connection to " + url, ex);
		}
		try {
			String ddl = "create table catalog ( c_key varchar(1000) primary key, c_entry clob)";
			Statement st = conn.createStatement();
			st.executeUpdate(ddl);
			closeStatement(st);
			conn.commit();								
		} catch (SQLException ex) {
			// Let's ignore errors for now
			// ex.printStackTrace();
			rollback();
		}
	}

	@Override
	public void close() {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException ex) {
			LOG.error("Cannot close JDBC connection", ex);
		}
	}
	/*
	 * Clean up the catalog database.
	 */
	void cleanup() throws CatalogException {
		Statement st = null;
		try {
			st = conn.createStatement();
			st.execute("delete from catalog");
			conn.commit();
		} catch (SQLException ex) {
			rollback();
			throw new CatalogException("Cannot delete all the catalog entries", ex);
		} finally {
			closeStatement(st);
		}
	}

	private void checkConnection() throws CatalogException {
		if (conn == null)
			throw new CatalogException("Catalog connection is not open.");
	}

	private void rollback() throws CatalogException {
		try {
			conn.rollback();
		} catch (SQLException ex) {
			throw new CatalogException("Cannot rollback JDBC transaction", ex);
		}
	}

	private void closeResultSet(ResultSet rs) throws CatalogException {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException ex) {
			throw new CatalogException("Cannot close JDBC ResultSet", ex);
		}
	}

	private void closeStatement(Statement st) throws CatalogException {
		try {
			if (st != null)
				st.close();
		} catch (SQLException ex) {
			throw new CatalogException("Cannot close JDBC Statement", ex);
		}
	}

	private void closeStream(InputStream input) {
		try {
			if (input != null)
				input.close();
		} catch (IOException ioex) {
			LOG.error("Cannot close input stream", ioex);
		}
	}
}