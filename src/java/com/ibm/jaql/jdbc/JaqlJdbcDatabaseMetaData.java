package com.ibm.jaql.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class JaqlJdbcDatabaseMetaData implements DatabaseMetaData
{
  protected final JaqlJdbcConnection conn;

  public JaqlJdbcDatabaseMetaData(JaqlJdbcConnection conn)
  {
    this.conn = conn;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException
  {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException
  {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException
  {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException
  {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
  {
    return false;
  }

  protected ResultSet emptyResult() throws SQLException
  {
    // TODO: statement isn't closed...
    return conn.createStatement().executeQuery("[]");
  }
  
  @Override
  public ResultSet getAttributes(
      String catalog, 
      String schemaPattern,
      String typeNamePattern,
      String attributeNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException
  {
    return null;
  }

  @Override
  public String getCatalogSeparator() throws SQLException
  {
    return "/";
  }

  @Override
  public String getCatalogTerm() throws SQLException
  {
    return "catalog";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema,
      String table, String columnNamePattern) throws SQLException
  {
    return emptyResult();
  }
  
  /**
   * Retrieves a description of table columns available in the specified catalog. 

Only column descriptions matching the catalog, schema, table and column name criteria are returned. They are ordered by TABLE_CAT,TABLE_SCHEM, TABLE_NAME, and ORDINAL_POSITION. 

Each column description has the following columns: 

TABLE_CAT String => table catalog (may be null) 
TABLE_SCHEM String => table schema (may be null) 
TABLE_NAME String => table name 
COLUMN_NAME String => column name 
DATA_TYPE int => SQL type from java.sql.Types 
TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified 
COLUMN_SIZE int => column size. 
BUFFER_LENGTH is not used. 
DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable. 
NUM_PREC_RADIX int => Radix (typically either 10 or 2) 
NULLABLE int => is NULL allowed. 
columnNoNulls - might not allow NULL values 
columnNullable - definitely allows NULL values 
columnNullableUnknown - nullability unknown 
REMARKS String => comment describing column (may be null) 
COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null) 
SQL_DATA_TYPE int => unused 
SQL_DATETIME_SUB int => unused 
CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column 
ORDINAL_POSITION int => index of column in table (starting at 1) 
IS_NULLABLE String => ISO rules are used to determine the nullability for a column. 
YES --- if the parameter can include NULLs 
NO --- if the parameter cannot include NULLs 
empty string --- if the nullability for the parameter is unknown 
SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF) 
SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF) 
SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF) 
SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF) 
IS_AUTOINCREMENT String => Indicates whether this column is auto incremented 
YES --- if the column is auto incremented 
NO --- if the column is not auto incremented 
empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown 
The COLUMN_SIZE column the specified column size for the given column. For numeric data, this is the maximum precision. For character data, this is the length in characters. For datetime datatypes, this is the length in characters of the String representation (assuming the maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data types where the column size is not applicable.


   */
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
      String tableNamePattern, String columnNamePattern) throws SQLException
  {
    // if(true) return emptyResult();
    if( schemaPattern != null &&
        ! "".equals(schemaPattern) &&
        ! "%".equals(schemaPattern ) )
    {
      throw new SQLFeatureNotSupportedException("schema name patterns are not supported");
    }
    String tableFilter = "";
    if( tableNamePattern != null &&
        ! "".equals(tableNamePattern) &&
        ! "%".equals(tableNamePattern ) )
    {
      if( tableNamePattern.indexOf('%') >= 0 )
      {
        throw new SQLFeatureNotSupportedException("table name patterns not supported");
      }
      tableFilter = " -> filter $.var == '" + tableNamePattern + "'"; // TODO: eliminate sql injection attack using variable + value.
    }
    if( columnNamePattern != null &&
        ! "".equals(columnNamePattern) &&
        ! "%".equals(columnNamePattern ) )
    {
      throw new SQLFeatureNotSupportedException("column name patterns not supported");
    }
    // TODO: support patterns
    // TODO: faster access by name
    return conn.createStatement().executeQuery(
        "listVariables() "+
        tableFilter +
        " -> filter $.isTable "+
        " -> expand each t ( t.schema -> elementsOf() -> fieldsOf() -> transform each c { t.var, c.name, c.schema, c.isOptional, c.index } ) "+
        " -> transform "+
        " (b = $.isOptional or isNullable($.schema), "+
        "  { TABLE_CAT: null, TABLE_SCHEM: null, TABLE_NAME: $.var, "+
        "    COLUMN_NAME: $.name, DATA_TYPE: sqlTypeCode($.schema), TYPE_NAME: $.schema, "+
        "    COLUMN_SIZE: null, BUFFER_LENGTH: null, "+
        "    DECIMAL_DIGITS: null, NUM_PREC_RADIX: 10, "+
        "    NULLABLE: if(b) 1 else if(not b) 0 else 2, "+
        "    REMARKS: null, COLUMN_DEF: null, SQL_DATA_TYPE: null,  SQL_DATETIME_SUB: null, "+
        "    CHAR_OCTET_LENGTH: -1, "+
        "    ORDINAL_POSITION: $.index, "+
        "    IS_NULLABLE: if(b) 'YES' else if(not b) 'NO' else null, "+
        "    SCOPE_CATLOG: null, SCOPE_SCHEMA: null, SCOPE_TABLE: null, SOURCE_DATA_TYPE: null , IS_AUTOINCREMENT: 'NO' })"
    );
  }
  
  @Override
  public Connection getConnection() throws SQLException
  {
    return conn;
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema,
      String parentTable, String foreignCatalog, String foreignSchema,
      String foreignTable) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException
  {
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException
  {
    return 5;
  }

  @Override
  public String getDatabaseProductName() throws SQLException
  {
    return "Jaql";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException
  {
    return "0.5";
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public int getDriverMajorVersion()
  {
    return 0;
  }

  @Override
  public int getDriverMinorVersion()
  {
    return 5;
  }

  @Override
  public String getDriverName() throws SQLException
  {
    return "Jaql JDBC talking Jaql";
  }

  @Override
  public String getDriverVersion() throws SQLException
  {
    return "0.5";
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException
  {
    return "";
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
      String functionNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException
  {
    return "\"";
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException
  {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException
  {
    return 1;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxStatementLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException
  {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException
  {
    return 0;
  }

  @Override
  public String getNumericFunctions() throws SQLException
  {
    return "";
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getProcedureTerm() throws SQLException
  {
    return "function";
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException
  {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public String getSQLKeywords() throws SQLException
  {
    return "";
  }

  @Override
  public int getSQLStateType() throws SQLException
  {
    return sqlStateSQL;
  }

  @Override
  public String getSchemaTerm() throws SQLException
  {
    return "schema";
  }

  @Override
  public ResultSet getSchemas() throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getSearchStringEscape() throws SQLException
  {
    return "";
  }

  @Override
  public String getStringFunctions() throws SQLException
  {
    return "";
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
      String typeNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getSystemFunctions() throws SQLException
  {
    return "";
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern,
      String tableNamePattern, String[] types) throws SQLException
  {
    if( catalog != null &&
        ! "".equals(catalog) &&
        ! "%".equals(catalog ) )
    {
      throw new SQLFeatureNotSupportedException("catalog names are not supported: "+catalog);
    }
    if( schemaPattern != null &&
        ! "".equals(schemaPattern) &&
        ! "%".equals(schemaPattern ) )
    {
      throw new SQLFeatureNotSupportedException("schema name patterns are not supported: "+schemaPattern);
    }
    String tableFilter = "";
    if( tableNamePattern != null &&
        ! "".equals(tableNamePattern) &&
        ! "%".equals(tableNamePattern ) )
    {
      if( tableNamePattern.indexOf('%') > 0 )
      {
        throw new SQLFeatureNotSupportedException("table name patterns not supported:"+tableNamePattern);
      }
      tableFilter = " -> filter $.var == '" + tableNamePattern + "'"; // TODO: eliminate sql injection attack using variable + value.
    }

    boolean tablesOrViews = false;
    for( String t: types )
    {
      if( "TABLE".equals(t) || "VIEW".equals(t) )
      {
        tablesOrViews = true;
        break;
      }
    }
    if( !tablesOrViews )
    {
      return emptyResult();
    }
    // if(true) return emptyResult();
    // TODO: returning bogus table to see if SPSS will lets us play.
    // TODO: statement isn't closed...
    /*
    TABLE_CAT String => table catalog (may be null) 
    TABLE_SCHEM String => table schema (may be null) 
    TABLE_NAME String => table name 
    TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM". 
    REMARKS String => explanatory comment on the table 
    TYPE_CAT String => the types catalog (may be null) 
    TYPE_SCHEM String => the types schema (may be null) 
    TYPE_NAME String => type name (may be null) 
    SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null) 
    REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null) 
     */
    // TODO: support patterns
    // TODO: faster access by name
    return conn.createStatement().executeQuery(
        // "T = range(10) -> transform { x: $, y: strcat('hi ',$) }; "+
        "listVariables() "+
        tableFilter +
        " -> filter $.isTable "+
        " -> transform "+
        " { TABLE_CAT: null, TABLE_SCHEM: null, TABLE_NAME: $.var, TABLE_TYPE: 'VIEW', "+
        "   REMARKS: 'variable view', "+
        "   TYPE_CAT: null, TYPE_SCHEM: null, TYPE_NAME: null, SELF_REFERENCING_COL_NAME: null, REF_GENERATION: null  }"
      );
  }

  @Override
  public String getTimeDateFunctions() throws SQLException
  {
    return "";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException
  {
    return emptyResult();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern,
      String typeNamePattern, int[] types) throws SQLException
  {
    return emptyResult();
  }

  @Override
  public String getURL() throws SQLException
  {
    return "jdbc:jaql:/";
  }

  @Override
  public String getUserName() throws SQLException
  {
    return "unknown";
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException
  {
    return emptyResult();
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException
  {
    return true;
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    return false;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException
  {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException
  {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException
  {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException
  {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException
  {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException
  {
    return true;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException
  {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
  {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException
  {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
  {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException
  {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException
  {
    return type == ResultSet.TYPE_FORWARD_ONLY && 
           (concurrency == ResultSet.FETCH_FORWARD || 
            concurrency == ResultSet.FETCH_UNKNOWN);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException
  {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException
  {
    return type == ResultSet.FETCH_FORWARD;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException
  {
    return level == Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsTransactions() throws SQLException
  {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException
  {
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException
  {
    return true;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException
  {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException
  {
    return true;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException
  {
    return false;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    return null;
  }

}
