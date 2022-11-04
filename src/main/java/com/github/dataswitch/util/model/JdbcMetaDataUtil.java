package com.github.dataswitch.util.model;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcMetaDataUtil {

	private static Logger _log = LoggerFactory.getLogger(JdbcMetaDataUtil.class);
	
	public SqlTable getSqlTableSqlColumns(Connection conn,String tableName) throws SQLException {
	      _log.debug("-------setSqlColumns(" + tableName + ")");

	      SqlTable table = new SqlTable();
	      table.setSqlName(tableName);
	      
	      // get the indices and unique columns
	      List indices = new LinkedList();
	      // maps index names to a list of columns in the index
	      Map uniqueIndices = new HashMap();
	      // maps column names to the index name.
	      
	      DatabaseMetaData metaData = conn.getMetaData();
	      ArrayList primaryKeys = new ArrayList();
	      Map uniqueSqlColumns = getIndexInfos(table, indices, uniqueIndices, metaData);

	      List columns = getSqlTableSqlColumns(conn,table, primaryKeys, indices, uniqueIndices, uniqueSqlColumns);

	      for (Iterator i = columns.iterator(); i.hasNext(); ) {
	         SqlColumn column = (SqlColumn)i.next();
	         table.addColumn(column);
	      }

	      // In case none of the columns were primary keys, issue a warning.
	      if (primaryKeys.size() == 0) {
	         _log.warn("WARNING: The JDBC driver didn't report any primary key columns in " + table.getSqlName());
	      }
	      return table;
	}

	private Map getIndexInfos(SqlTable table, List indices, Map uniqueIndices,
			DatabaseMetaData metaData) throws SQLException {
		Map uniqueSqlColumns = new HashMap();
		ResultSet indexRs = null;
		try {
	         indexRs = metaData.getIndexInfo(null, null, table.getSqlName(), false, true);
	         
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
	               List l = (List)uniqueSqlColumns.get(indexName);
	               if (l == null) {
	                  l = new ArrayList();
	                  uniqueSqlColumns.put(indexName, l);
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
		return uniqueSqlColumns;
	}

	private List<SqlColumn> getSqlTableSqlColumns(Connection conn,SqlTable table, List primaryKeys, List indices, Map uniqueIndices, Map uniqueSqlColumns) throws SQLException {
		// get the columns
		List columns = new LinkedList();
		ResultSet columnRs = getSqlColumnsResultSet(conn.getMetaData(),table.getSqlName());
		  
		while (columnRs.next()) {
			SqlColumn column = buildSqlColumn(table, primaryKeys, indices, uniqueIndices, uniqueSqlColumns, columnRs);
			columns.add(column);
		}
		  
		columnRs.close();
		return columns;
	}

	private SqlColumn buildSqlColumn(SqlTable table, List primaryKeys, List indices, Map uniqueIndices,
			Map uniqueSqlColumns, ResultSet columnRs) throws SQLException {
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
		    columnsInUniqueIndex = (List)uniqueSqlColumns.get(uniqueIndex);
		 }

		 boolean isUnique = columnsInUniqueIndex != null && columnsInUniqueIndex.size() == 1;
		 if (isUnique) {
		    _log.debug("unique column:" + columnName);
		 }
		 
		 SqlColumn column = new SqlColumn(
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
		return column;
	}

	private ResultSet getSqlColumnsResultSet(DatabaseMetaData metaData,String tableName) throws SQLException {
		return metaData.getColumns(null, null, tableName, null);
	}

	
}
