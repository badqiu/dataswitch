package com.github.dataswitch.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.sql.DataSource;

import com.github.dataswitch.util.PropertyUtils;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

public class JdbcUtil {
	private static Logger logger = LoggerFactory.getLogger(JdbcUtil.class);
	
    public static Map<String,Map> tableColumnsCache = new HashMap<String,Map>();
    
	private static Map getMissColumns(JdbcTemplate jdbcTemplate, Map columnsWithData, String table,String jdbcUrl) {
        Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table,jdbcUrl);
        return MapUtil.getDifferenceMap(MapUtil.keyToLowerCase(tableColumns), MapUtil.keyToLowerCase(columnsWithData));
	}
	
	public static BiConsumer<String, String> newAlterTableAddColumnFunction(JdbcTemplate jdbcTemplate,String table) {
		BiConsumer<String,String> alterTableAddColumnAction = (columnName, jdbcSqlType) -> {
			long start = System.currentTimeMillis();
			String alterSql = "ALTER TABLE "+table+"  ADD COLUMN `"+columnName+"` "+jdbcSqlType;
			jdbcTemplate.execute(alterSql);
			long cost = start - System.currentTimeMillis();
			logger.info("executed alter_table_add_column sql:["+alterSql+"], costSeconds:"+(cost/1000));
		};
		return alterTableAddColumnAction;
	}
	
	public static void alterTableIfColumnMiss(final JdbcTemplate jdbcTemplate, Map columnsWithData, String table,String jdbcUrl,Map<String,String> columnsSqlType,String defaultColumnSqlType) {
        BiConsumer<String,String> alterTableAddColumnAction = newAlterTableAddColumnFunction(jdbcTemplate,table);
        alterTableIfColumnMiss(jdbcTemplate,columnsWithData,table,jdbcUrl,columnsSqlType,defaultColumnSqlType,alterTableAddColumnAction);
	}
	
	public static void alterTableIfColumnMiss(JdbcTemplate jdbcTemplate, Map columnsWithData, String table,String jdbcUrl,Map<String,String> columnsSqlType,String defaultColumnSqlType,BiConsumer<String,String> alterTableAddColumnAction) {
		Map missColumns = getMissColumns(jdbcTemplate, columnsWithData, table,jdbcUrl);
        if (missColumns == null) return;
        
        try {
	        Map sqlTypes = JdbcDataTypeUtil.getDatabaseDataType(jdbcUrl, missColumns,columnsSqlType,defaultColumnSqlType);	        
			sqlTypes.forEach(alterTableAddColumnAction);
        }finally {
        	removeTableColumnsCache(table, jdbcUrl);
        }
	}

	private static void removeTableColumnsCache(String tableName, String jdbcUrl) {
		String cacheKey = JdbcUtil.getTableCacheKey(tableName, jdbcUrl);
        JdbcUtil.tableColumnsCache.remove(cacheKey);
	}
	
	public static String getTableCacheKey(String tableName, String jdbcUrl) {
		String cacheKey = jdbcUrl + tableName;
		return cacheKey;
	}
    
	/**
	 * 获得table的列名及列类型
	 *  */
    public static Map<String,String> getTableColumns(JdbcTemplate jdbcTemplate, String tableName,String jdbcUrl) {
    	String cacheKey = getTableCacheKey(tableName, jdbcUrl);
    	Map<String,String> tableColumns = tableColumnsCache.get(cacheKey);
    	if( tableColumns == null) {
    		tableColumns = getTableColumnsFromConnectionMetaData(tableName,jdbcTemplate);
    		
    		synchronized (tableColumnsCache) {
            	tableColumnsCache.put(cacheKey, tableColumns);
			}
    	}
    	
        if (MapUtils.isEmpty(tableColumns)) {
        	
        	String sql = "select * from  " + tableName + " limit 1 ";
        	if(jdbcUrl.contains("oracle") || jdbcUrl.contains("sqlserver")) {
        		sql = "select * from  " + tableName;
        	}
        	
            SqlRowSet srs = jdbcTemplate.queryForRowSet(sql);
            tableColumns = JdbcUtil.getSqlColumnsNameType(srs);
            
            synchronized (tableColumnsCache) {
            	tableColumnsCache.put(cacheKey, tableColumns);
			}
        }
        return tableColumns;
    }
    
    public static List<String> getTableColumnsName(JdbcTemplate jdbcTemplate, String tableName,String jdbcUrl) {
    	Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, tableName, jdbcUrl);
    	ArrayList columns = new ArrayList(MapUtil.keyToLowerCase(tableColumns).keySet());
    	return columns;
    }
   
	
	private static Map<String, String> getTableColumnsFromConnectionMetaData(String tableName, JdbcTemplate jt) {
		final Map<String,String> columnMap = new LinkedHashMap();
		jt.execute(new ConnectionCallback<Object>() {
			@Override
			public Object doInConnection(Connection con) throws SQLException, DataAccessException {
				ResultSet rs = con.getMetaData().getColumns(null, null, tableName, null);
				
				rs2ColumnsMap(tableName, columnMap, rs);
				return null;
			}
		});
		
		return columnMap;
	}
	
	public static Map<String, String> getTablePrimaryKeys(String tableName, JdbcTemplate jt) {
		final Map<String,String> columnMap = new LinkedHashMap();
		jt.execute(new ConnectionCallback<Object>() {
			@Override
			public Object doInConnection(Connection con) throws SQLException, DataAccessException {
				ResultSet rs = con.getMetaData().getPrimaryKeys(null, null, tableName);
				rs2ColumnsMap(tableName, columnMap, rs);
				return null;
			}

		});
		
		return columnMap;
	}
	
	public static List<String> getTablePrimaryKeysList(String tableName, JdbcTemplate jt) {
		return new ArrayList(getTablePrimaryKeys(tableName,jt).keySet());
	}
	
	private static void rs2ColumnsMap(String tableName, final Map<String, String> columnMap, ResultSet rs)
			throws SQLException {
		List<Map<String,Object>> columns = resultSet2List(rs);
		
		for(Map column : columns) {
			String TABLE_NAME = (String)column.get("TABLE_NAME");
			if(!tableName.equalsIgnoreCase(TABLE_NAME)) {
				continue;
			}
			
			String TYPE_NAME = (String)column.get("TYPE_NAME");
			String COLUMN_NAME = (String)column.get("COLUMN_NAME");
			Integer DATA_TYPE = (Integer)column.get("DATA_TYPE");
			Integer COLUMN_SIZE = (Integer)column.get("COLUMN_SIZE");
			String IS_NULLABLE = (String)column.get("IS_NULLABLE");
			Integer DECIMAL_DIGITS = (Integer)column.get("DECIMAL_DIGITS");
			columnMap.put(COLUMN_NAME.toLowerCase(), TYPE_NAME);
		}
	}
	
	public static List<Map<String,Object>> resultSet2List(ResultSet rs) throws SQLException {
		RowMapperResultSetExtractor<Map<String, Object>> rse =
				new RowMapperResultSetExtractor<Map<String, Object>>(new ColumnMapRowMapper());
		List<Map<String,Object>> rows = rse.extractData(rs);
		return rows;
	}
	
    
    public static Map<String,String> getSqlColumnsNameType(SqlRowSet srs) {
        return getSqlColumnsNameType(srs,false);
    }
    
    public static Map<String,String> getSqlColumnsNameType(SqlRowSet srs,boolean nameToLowerCase) {
        SqlRowSetMetaData metaData = srs.getMetaData();
        Map result = new LinkedHashMap();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            if(nameToLowerCase) {
            	columnName = columnName.toLowerCase();
            }
			result.put(columnName, metaData.getColumnTypeName(i));
        }
        return result;
    }
    
	
	public static String getJdbcUrl(DataSource dataSource)  {
		String result = null;
		if(StringUtils.isBlank(result)) {
			try {
				Connection connection = null;
				try {
					connection = dataSource.getConnection();
					result = connection.getMetaData().getURL();
				}finally {
					if(connection != null) {
						connection.close();
					}
				}
			}catch(Exception e) {
				throw new RuntimeException("cannot get jdbc url by jdbc connection",e);
			}
		}
		
		return result;
	}
	
	public static String getReplacedSql(ParsedSql parsedSql, Object row) {
		String replacedSql = parsedSql.getOriginalSql();
		List<String> parameterNames = new ArrayList(parsedSql.getParameterNames());
		Collections.sort(parameterNames,ComparatorUtils.reversedComparator(null));
		
		for(String name : parameterNames) {
			Object value = null;
			try {
				value = PropertyUtils.getSimpleProperty(row, name);
			}catch(Exception e) {
				throw new RuntimeException("error, name:"+name+" on sql:"+parsedSql.getOriginalSql()+",row:"+row,e);
			}
			
			if(value == null) throw new RuntimeException("not found value for name:"+name+" on sql:"+parsedSql.getOriginalSql()+",row:"+row);
			
			replacedSql = StringUtils.replace(replacedSql, ":"+name,getReplacedValue(value));
		}
		return replacedSql;
	}

	private static String getReplacedValue(Object value) {
		if(value == null) return "";
		
		if(value instanceof String) {
			return "'"+value+"'";
		}
		return String.valueOf(value);
	}

	public static boolean tableExists(JdbcTemplate jdbcTemplate, String table,String jdbcUrl) {
		try {
			Map<String,String> columns = getTableColumns(jdbcTemplate, table, jdbcUrl);
			return !columns.isEmpty();
		}catch(Exception e) {
			return false;
		}
	}
	
	static Map<String,String[]> splitTableColumnsCache = new HashMap();
	public static String[] splitTableColumns(String columns) {
		if(StringUtils.isBlank(columns)) return null;
		
		String[] results = splitTableColumnsCache.get(columns);
		if(results == null) {
			results = org.springframework.util.StringUtils.tokenizeToStringArray(columns, " ,\t\n，");
			synchronized (splitTableColumnsCache) {
				splitTableColumnsCache.put(columns, results);
			}
		}
		
		return results;
	}
	
	public static final String SQL_SEPARATOR_CHAR = ";";
	public static void executeWithSemicolonComma(DataSource ds, String sql) {
		if (StringUtils.isBlank(sql)) {
			return;
		}
		
		final String[] sqls = StringUtils.split(sql,SQL_SEPARATOR_CHAR);;
		for (String s : sqls) {
			if(StringUtils.isBlank(s)) {
				continue;
			}
			
			new JdbcTemplate(ds).execute(s);
		}
	}
	
	/**
	 * 修复有些jdbc，列名表表名的问题。    
	 * @param columnName  some_table.some_column
	 * @return some_column
	 */
	public static String getFinalColumnKey(String columnName) {
		if(columnName == null) return null;
		
		int index =  columnName.lastIndexOf(".");
		if(index >= 0) {
			return columnName.substring(index + 1, columnName.length());
		}
		return columnName;
	};
}
