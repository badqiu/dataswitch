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

import javax.sql.DataSource;

import org.apache.commons.collections.ComparatorUtils;
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

import com.github.rapid.common.beanutils.PropertyUtils;

public class JdbcUtil {
	private static Logger logger = LoggerFactory.getLogger(JdbcUtil.class);
	
    public static Map<String,Map> tableColumnsCache = new HashMap<String,Map>();
    
	private static Map getMissColumns(JdbcTemplate jdbcTemplate, Map allMap, String table,String jdbcUrl) {
        Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table,jdbcUrl);
        return MapUtil.getDifferenceMap(MapUtil.keyToLowerCase(tableColumns), MapUtil.keyToLowerCase(allMap));
	}
	
	public static void alterTableIfColumnMiss(JdbcTemplate jdbcTemplate, Map allMap, String table,String jdbcUrl) {
		Map missColumns = getMissColumns(jdbcTemplate, allMap, table,jdbcUrl);
        if (missColumns == null) return;
        
        Map sqlTypes = JdbcDataTypeUtil.getDatabaseDataType(jdbcUrl, missColumns);
        sqlTypes.forEach((key, jdbcType) -> {
        	long start = System.currentTimeMillis();
        	String sql = "ALTER TABLE "+table+"  ADD COLUMN `"+key+"` "+jdbcType;
        	jdbcTemplate.execute(sql);
        	long cost = start - System.currentTimeMillis();
        	logger.info("executed alter_table_add_column sql:["+sql+"], costSeconds:"+(cost/1000));
        });
        
        String cacheKey = JdbcUtil.getTableCacheKey(table, jdbcUrl);
        JdbcUtil.tableColumnsCache.remove(cacheKey);
	}
	
	public static String getTableCacheKey(String tableName, String jdbcUrl) {
		String cacheKey = jdbcUrl + tableName;
		return cacheKey;
	}
    
    public static Map<String,String> getTableColumns(JdbcTemplate jdbcTemplate, String tableName,String jdbcUrl) {
    	String cacheKey = getTableCacheKey(tableName, jdbcUrl);
    	Map<String,String> tableColumns = tableColumnsCache.get(cacheKey);
        if (tableColumns == null) {
//        	String sql = "select * from  " + tableName + " limit 1 ";
            String sql = "select * from  " + tableName;
            SqlRowSet srs = jdbcTemplate.queryForRowSet(sql);
            tableColumns = JdbcUtil.getSqlColumnsNameType(srs);
            synchronized (tableColumnsCache) {
            	tableColumnsCache.put(cacheKey, tableColumns);
			}
        }
        return tableColumns;
    }
    
	private static Map<String, Object> getTableColumns(String tableName, JdbcTemplate jt) {
		final Map<String,Object> columnMap = new LinkedHashMap();
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
	
	public static Map<String, Object> getTablePrimaryKeys(String tableName, JdbcTemplate jt) {
		final Map<String,Object> columnMap = new LinkedHashMap();
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
	
	private static void rs2ColumnsMap(String tableName, final Map<String, Object> columnMap, ResultSet rs)
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
			Object value = PropertyUtils.getSimpleProperty(row, name);
			
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
	
	
	
	
}
