package com.github.dataswitch.util;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

public class JdbcUtil {

    public static Map<String,Map> tableColumnsCache = new HashMap<String,Map>();
    
	public static String getTableCacheKey(String tableName, String jdbcUrl) {
		String cacheKey = jdbcUrl + tableName;
		return cacheKey;
	}
    
    public static Map<String,String> getTableColumns(JdbcTemplate jdbcTemplate, String tableName,String jdbcUrl) {
    	String cacheKey = getTableCacheKey(tableName, jdbcUrl);
    	Map<String,String> tableColumns = tableColumnsCache.get(cacheKey);
        if (tableColumns == null) {
            String sql = "select * from  " + tableName + " limit 1 ";
            SqlRowSet srs = jdbcTemplate.queryForRowSet(sql);
            tableColumns = JdbcUtil.getSqlColumnsNameType(srs);
            synchronized (tableColumnsCache) {
            	tableColumnsCache.put(cacheKey, tableColumns);
			}
        }
        return tableColumns;
    }
    
    public static Map<String,String> getSqlColumnsNameType(SqlRowSet srs) {
        return getSqlColumnsNameType(srs,false);
    }
    
    public static Map<String,String> getSqlColumnsNameType(SqlRowSet srs,boolean nameToLowerCase) {
        SqlRowSetMetaData metaData = srs.getMetaData();
        Map result = new HashMap();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            if(nameToLowerCase) {
            	columnName = columnName.toLowerCase();
            }
			result.put(columnName, metaData.getColumnTypeName(i));
        }
        return result;
    }
    
	
	public static String generateInsertSqlByColumns(String table, Collection<String> allColumns) {
		if(CollectionUtils.isEmpty(allColumns))
			return null;
		
		StringJoiner valueJoiner = new StringJoiner(",");
		StringJoiner keyJoiner = new StringJoiner(",");
		allColumns.forEach((columnName) -> {
			if(StringUtils.isBlank(columnName)) return;
			
			valueJoiner.add(":"+columnName);
			keyJoiner.add("`"+columnName+"`");
		});
		
		String sql = "insert into " + table + " ("+keyJoiner.toString()+") values ("+valueJoiner.toString()+")";
		return sql;
	}


}
