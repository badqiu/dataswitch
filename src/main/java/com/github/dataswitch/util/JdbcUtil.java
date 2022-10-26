package com.github.dataswitch.util;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

public class JdbcUtil {

    public static Map<String,Map> tableColumnsCache = new HashMap<String,Map>();
    
	public static String getTableCacheKey(String tableName, String jdbcUrl) {
		String cacheKey = jdbcUrl + tableName;
		return cacheKey;
	}
    
    public static Map getTableColumns(JdbcTemplate jdbcTemplate, String tableName,String jdbcUrl) {
    	String cacheKey = getTableCacheKey(tableName, jdbcUrl);
    	Map tableColumns = tableColumnsCache.get(cacheKey);
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
        SqlRowSetMetaData metaData = srs.getMetaData();
        Map result = new HashMap();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            result.put(metaData.getColumnName(i), metaData.getColumnTypeName(i));
        }
        return result;
    }
    
	
	public static String generateInsertSqlByColumns(String table, Collection<String> allColumns) {
		if(CollectionUtils.isEmpty(allColumns))
			return null;
		
		StringJoiner valueJoiner = new StringJoiner(",", "", "");
		allColumns.forEach((item) -> {
			valueJoiner.add(":"+item);
		});
		
		String sql = "insert into " + table + " ("+joinColumns(allColumns)+") values ("+valueJoiner.toString()+")";
		return sql;
	}

	public static String joinColumns(Collection<String> list) {
		if(CollectionUtils.isEmpty(list)) return "";
		
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		for(String item : list) {
			item = "`"+item+"`";
			if(isFirst) {
				isFirst = false;
				sb.append(item);
			}else {
				sb.append(",").append(item);
			}
		}
		return sb.toString();
	}


}
