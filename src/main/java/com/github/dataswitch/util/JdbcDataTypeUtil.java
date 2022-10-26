package com.github.dataswitch.util;

import java.util.Date;

public class JdbcDataTypeUtil {

	public static String getDatabaseDataType(String url,Object value) {
		if(url.contains("mysql") || url.contains("mariadb")) {
			return JdbcDataTypeUtil.getMysqlDataType(value);
		}else if(url.contains("clickhouse")) {
			return JdbcDataTypeUtil.getClickHouseDataType(value);
		}else if(url.contains("sqlserver")) {
			return JdbcDataTypeUtil.getSqlServerDataType(value);
		}else if(url.contains("oracle")) {
			return JdbcDataTypeUtil.getOracleDataType(value);	
		}else if(url.contains("postgresql")) {
			return JdbcDataTypeUtil.getPostgreSQLDataType(value);	
		}else if(url.contains("hive2")) {
			return JdbcDataTypeUtil.getHiveDataType(value);
		}else if(url.contains("jdbc:h2:")) {
			return JdbcDataTypeUtil.getH2DataType(value);			
		}else {
			throw new UnsupportedOperationException("cannot get database type by url:"+url);
		}
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
        if (value == null) {
            return "VARCHAR(200)";
        }

        if (value instanceof String) {
            return "VARCHAR(200)";
        }

		if(value instanceof Double) return "NUMERIC(20, 2)";
		if(value instanceof Float) return "NUMERIC(20, 2)";
		if(value.getClass() == double.class) return "NUMERIC(20, 2)";
		if(value.getClass() == float.class) return "NUMERIC(20, 2)";
		
		if(value instanceof Number) return "BIGINT";
		if(value instanceof Date) return "TIMESTAMP";

        return "VARCHAR(200)";		
	}
	
}
