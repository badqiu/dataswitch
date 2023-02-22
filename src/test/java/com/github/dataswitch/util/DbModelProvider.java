package com.github.dataswitch.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.dataswitch.util.model.Column;
import com.github.dataswitch.util.model.Table;
/**
 * 
 * @author badqiu
 * @email badqiu(a)gmail.com
 */
public class DbModelProvider {
	/**
	 * Logger for this class
	 */
	private static final Log _log = LogFactory.getLog(DbModelProvider.class);

//	Properties props;
	public String catalog;
	public String schema;
	
	private DataSource dataSource;
	
	public DbModelProvider(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public List<Table> getAllTables() throws Exception {
		Connection conn = dataSource.getConnection();
		return getAllTables(conn);
	}
	
	public Table getTable(String sqlTableName) throws Exception {
		Connection conn = dataSource.getConnection();
		DatabaseMetaData dbMetaData = conn.getMetaData();
		ResultSet rs = dbMetaData.getTables(catalog, schema, sqlTableName, null);
		while(rs.next()) {
			Table table = createTable(conn, rs);
			return table;
		}
		throw new RuntimeException("not found table with give name:"+sqlTableName);
	}

	private Table createTable(Connection conn, ResultSet rs) throws SQLException {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		String schemaName = rs.getString("TABLE_SCHEM") == null ? "" : rs.getString("TABLE_SCHEM");
		String realTableName = rs.getString("TABLE_NAME");
		String tableType = rs.getString("TABLE_TYPE");
		String remarks = rs.getString("REMARKS");
		if(StringUtils.isBlank(remarks)) {
			remarks = getTableRemarks(conn,schemaName,realTableName);
		}
		
		Table table = new Table();
		table.setSqlName(realTableName);
		if ("SYNONYM".equals(tableType) && isOracleDataBase()) {
		    table.setOwnerSynonymName(getSynonymOwner(realTableName));
		}
		table.setRemarks(remarks);
		
		retriveTableColumns(table);
		
		return table;
	}
	
	public String getTableRemarks(Connection conn,String tableSchema,String table) throws SQLException {
		String sql = "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE  TABLE_NAME = '"+table+"'";
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while (rs.next()) {
		    String tableRemark = rs.getString(1);
		    return tableRemark;
		}
		return null;
	}
	
	public List<Table> getAllTables(Connection conn) throws SQLException {
		DatabaseMetaData dbMetaData = conn.getMetaData();
		ResultSet rs = dbMetaData.getTables(catalog, schema, null, null);
		List tables = new ArrayList();
		while(rs.next()) {
			Table table = createTable(conn, rs);
			tables.add(table);
		}
		return tables;
	}

	private boolean isOracleDataBase() {
		boolean ret = false;
		try {
			ret = (getMetaData().getDatabaseProductName().toLowerCase()
					.indexOf("oracle") != -1);
		} catch (Exception ignore) {
		}
		return ret;
	}
	   
   private String getSynonymOwner(String synonymName)  {
	      PreparedStatement ps = null;
	      ResultSet rs = null;
	      String ret = null;
	      try {
	         ps = dataSource.getConnection().prepareStatement("select table_owner from sys.all_synonyms where table_name=? and owner=?");
	         ps.setString(1, synonymName);
	         ps.setString(2, schema);
	         rs = ps.executeQuery();
	         if (rs.next()) {
	            ret = rs.getString(1);
	         }
	         else {
	            String databaseStructure = getDatabaseStructureInfo();
	            throw new RuntimeException("Wow! Synonym " + synonymName + " not found. How can it happen? " + databaseStructure);
	         }
	      } catch (SQLException e) {
	         String databaseStructure = getDatabaseStructureInfo();
	         _log.error(e.getMessage(), e);
	         throw new RuntimeException("Exception in getting synonym owner " + databaseStructure);
	      } finally {
	         if (rs != null) {
	            try {
	               rs.close();
	            } catch (Exception e) {
	            }
	         }
	         if (ps != null) {
	            try {
	               ps.close();
	            } catch (Exception e) {
	            }
	         }
	      }
	      return ret;
	   }
   
   private String getDatabaseStructureInfo() {
	      ResultSet schemaRs = null;
	      ResultSet catalogRs = null;
	      String nl = System.getProperty("line.separator");
	      StringBuffer sb = new StringBuffer(nl);
	      // Let's give the user some feedback. The exception
	      // is probably related to incorrect schema configuration.
	      sb.append("Configured schema:").append(schema).append(nl);
	      sb.append("Configured catalog:").append(catalog).append(nl);

	      try {
	         schemaRs = getMetaData().getSchemas();
	         sb.append("Available schemas:").append(nl);
	         while (schemaRs.next()) {
	            sb.append("  ").append(schemaRs.getString("TABLE_SCHEM")).append(nl);
	         }
	      } catch (SQLException e2) {
	         _log.warn("Couldn't get schemas", e2);
	         sb.append("  ?? Couldn't get schemas ??").append(nl);
	      } finally {
	         try {
	            schemaRs.close();
	         } catch (Exception ignore) {
	         }
	      }

	      try {
	         catalogRs = getMetaData().getCatalogs();
	         sb.append("Available catalogs:").append(nl);
	         while (catalogRs.next()) {
	            sb.append("  ").append(catalogRs.getString("TABLE_CAT")).append(nl);
	         }
	      } catch (SQLException e2) {
	         _log.warn("Couldn't get catalogs", e2);
	         sb.append("  ?? Couldn't get catalogs ??").append(nl);
	      } finally {
	         try {
	            catalogRs.close();
	         } catch (Exception ignore) {
	         }
	      }
	      return sb.toString();
    }
	   
	private DatabaseMetaData getMetaData() throws SQLException {
		return dataSource.getConnection().getMetaData();
	}
	
	private void retriveTableColumns(Table table) throws SQLException {
	      _log.debug("-------setColumns(" + table.getSqlName() + ")");

	      List primaryKeys = getTablePrimaryKeys(table);
	      table.setPrimaryKeyColumns(primaryKeys);
	      
	      // get the indices and unique columns
	      List indices = new LinkedList();
	      // maps index names to a list of columns in the index
	      Map uniqueIndices = new HashMap();
	      // maps column names to the index name.
	      Map uniqueColumns = new HashMap();
	      ResultSet indexRs = null;

	      try {

	         if (table.getOwnerSynonymName() != null) {
	            indexRs = getMetaData().getIndexInfo(catalog, table.getOwnerSynonymName(), table.getSqlName(), false, true);
	         }
	         else {
	            indexRs = getMetaData().getIndexInfo(catalog, schema, table.getSqlName(), false, true);
	         }
	         while (indexRs.next()) {
	            String columnName = indexRs.getString("COLUMN_NAME");
	            if (columnName != null) {
	               _log.debug("index:" + columnName);
	               indices.add(columnName);
	            }

	            // now look for unique columns
	            String indexName = indexRs.getString("INDEX_NAME");
	            boolean nonUnique = indexRs.getBoolean("NON_UNIQUE");

	            if (!nonUnique && columnName != null && indexName != null) {
	               List l = (List)uniqueColumns.get(indexName);
	               if (l == null) {
	                  l = new ArrayList();
	                  uniqueColumns.put(indexName, l);
	               }
	               l.add(columnName);
	               uniqueIndices.put(columnName, indexName);
	               _log.debug("unique:" + columnName + " (" + indexName + ")");
	            }
	         }
	      } catch (Throwable t) {
	         // Bug #604761 Oracle getIndexInfo() needs major grants
	         // http://sourceforge.net/tracker/index.php?func=detail&aid=604761&group_id=36044&atid=415990
	      } finally {
	         if (indexRs != null) {
	            indexRs.close();
	         }
	      }
	      int uniqueIndexMaxColumns = getUniqueIndexMaxColumns(uniqueColumns);
	      List columns = getTableColumns(table, primaryKeys, indices, uniqueIndices, uniqueColumns);

	      for (Iterator i = columns.iterator(); i.hasNext(); ) {
	         Column column = (Column)i.next();
	         table.addColumn(column);
	      }
	      table.setUniqueIndexs(uniqueColumns);
	      table.setUniqueIndexMaxColumns(uniqueIndexMaxColumns);

	      // In case none of the columns were primary keys, issue a warning.
	      if (primaryKeys.size() == 0) {
	         _log.warn("WARNING: The JDBC driver didn't report any primary key columns in " + table.getSqlName());
	      }
	}

	private int getUniqueIndexMaxColumns(Map<String,List> uniqueColumns) {
		AtomicInteger result = new AtomicInteger();
		uniqueColumns.forEach((column,list) -> {
			if(result.get() < list.size()) {
				result.set(list.size());
			}
		});
		
		return result.get();
	}

	private List getTableColumns(Table table, List primaryKeys, List indices, Map uniqueIndices, Map uniqueColumns) throws SQLException {
		// get the columns
	      List columns = new LinkedList();
	      ResultSet columnRs = getColumnsResultSet(table);
	      
	      while (columnRs.next()) {
	         int sqlType = columnRs.getInt("DATA_TYPE");
	         String sqlTypeName = columnRs.getString("TYPE_NAME");
	         String columnName = columnRs.getString("COLUMN_NAME");
	         String columnDefaultValue = columnRs.getString("COLUMN_DEF");
	         // if columnNoNulls or columnNullableUnknown assume "not nullable"
	         boolean isNullable = (DatabaseMetaData.columnNullable == columnRs.getInt("NULLABLE"));
	         int size = columnRs.getInt("COLUMN_SIZE");
	         int decimalDigits = columnRs.getInt("DECIMAL_DIGITS");

	         boolean isPk = primaryKeys.contains(columnName);
	         boolean isIndexed = indices.contains(columnName);
	         String uniqueIndex = (String)uniqueIndices.get(columnName);
	         List columnsInUniqueIndex = null;
	         if (uniqueIndex != null) {
	            columnsInUniqueIndex = (List)uniqueColumns.get(uniqueIndex);
	         }

	         boolean isUnique = columnsInUniqueIndex != null && columnsInUniqueIndex.size() == 1;
	         if (isUnique) {
	            _log.debug("unique column:" + columnName+" columnsInUniqueIndex:"+columnsInUniqueIndex);
	         }
	         Column column = new Column(
	               table,
	               sqlType,
	               sqlTypeName,
	               columnName,
	               size,
	               decimalDigits,
	               isPk,
	               isNullable,
	               isIndexed,
	               isUnique,
	               columnDefaultValue);
	         columns.add(column);
	      }
	      columnRs.close();
		return columns;
	}

	private ResultSet getColumnsResultSet(Table table) throws SQLException {
		ResultSet columnRs = null;
	      if (table.getOwnerSynonymName() != null) {
	         columnRs = getMetaData().getColumns(catalog, table.getOwnerSynonymName(), table.getSqlName(), null);
	      }
	      else {
	         columnRs = getMetaData().getColumns(catalog, schema, table.getSqlName(), null);
	      }
		return columnRs;
	}

	private List getTablePrimaryKeys(Table table) throws SQLException {
		// get the primary keys
	      List primaryKeys = new LinkedList();
	      ResultSet primaryKeyRs = null;
	      if (table.getOwnerSynonymName() != null) {
	         primaryKeyRs = getMetaData().getPrimaryKeys(catalog, table.getOwnerSynonymName(), table.getSqlName());
	      }
	      else {
	         primaryKeyRs = getMetaData().getPrimaryKeys(catalog, schema, table.getSqlName());
	      }
	      while (primaryKeyRs.next()) {
	         String columnName = primaryKeyRs.getString("COLUMN_NAME");
	         _log.debug("primary key:" + columnName);
	         primaryKeys.add(columnName);
	      }
	      primaryKeyRs.close();
		return primaryKeys;
	}
	
}
