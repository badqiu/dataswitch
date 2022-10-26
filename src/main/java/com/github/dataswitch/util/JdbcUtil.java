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
    
    public static String getClickHouseDataType(Object value) {
        if (value == null) {
            return "String";
        }

        if (value instanceof String) {
            return "String";
        }

		if(value instanceof Double) return "Float64";
		if(value instanceof Float) return "Float32";
		if(value.getClass() == double.class) return "Float64";
		if(value.getClass() == float.class) return "Float32";
		
		if(value instanceof Number) return "Int64";
		if(value instanceof Date) return "DateTime64(3)";

        return "String";
    }
    
    public static String getMysqlDataType(Object value) {
        if (value == null) {
            return "varchar(200)";
        }

        if (value instanceof String) {
            return "varchar(200)";
        }

		if(value instanceof Double) return "DOUBLE";
		if(value instanceof Float) return "FLOAT";
		if(value.getClass() == double.class) return "DOUBLE";
		if(value.getClass() == float.class) return "FLOAT";
		
		if(value instanceof Number) return "BIGINT";
		if(value instanceof Date) return "DATETIME";

        return "varchar(200)";
    }
    
    public static String getOracleDataType(Object value) {
        if (value == null) {
            return "VARCHAR2(200)";
        }

        if (value instanceof String) {
            return "VARCHAR2(200)";
        }

		if(value instanceof Double) return "FLOAT";
		if(value instanceof Float) return "FLOAT";
		if(value.getClass() == double.class) return "FLOAT";
		if(value.getClass() == float.class) return "FLOAT";
		
		if(value instanceof Number) return "INTEGER";
		if(value instanceof Date) return "DATE";

        return "VARCHAR2(200)";
    }
    
    public static String getSqlServerDataType(Object value) {
        if (value == null) {
            return "VARCHAR(200)";
        }

        if (value instanceof String) {
            return "VARCHAR(200)";
        }

		if(value instanceof Double) return "FLOAT";
		if(value instanceof Float) return "FLOAT";
		if(value.getClass() == double.class) return "FLOAT";
		if(value.getClass() == float.class) return "FLOAT";
		
		if(value instanceof Number) return "BIGINT";
		if(value instanceof Date) return "DATETIME";

        return "VARCHAR(200)";
    }
    
    public static String getPostgreSQLDataType(Object value) {
        if (value == null) {
            return "VARCHAR(200)";
        }

        if (value instanceof String) {
            return "VARCHAR(200)";
        }

		if(value instanceof Double) return "float8";
		if(value instanceof Float) return "float8";
		if(value.getClass() == double.class) return "float8";
		if(value.getClass() == float.class) return "float8";
		
		if(value instanceof Number) return "BIGINT";
		if(value instanceof Date) return "timestamp";

        return "VARCHAR(200)";
    }

	public static String getHiveDataType(Object value) {
        if (value == null) {
            return "STRING";
        }

        if (value instanceof String) {
            return "STRING";
        }

		if(value instanceof Double) return "DOUBLE";
		if(value instanceof Float) return "FLOAT";
		if(value.getClass() == double.class) return "DOUBLE";
		if(value.getClass() == float.class) return "FLOAT";
		
		if(value instanceof Number) return "BIGINT";
		if(value instanceof Date) return "TIMESTAMP";

        return "STRING";
	}    
    
	public static String getH2DataType(Object value) {
        return getMysqlDataType(value);
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
