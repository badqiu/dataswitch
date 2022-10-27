package com.github.dataswitch.util;

import java.util.Date;

import org.springframework.util.Assert;

public class JdbcDataTypeUtil {

	private static final String VARCHAR = "VARCHAR(4000)";

	public static String getDatabaseDataType(String url,Object value) {
		Assert.hasText(url,"jdbc url must be not blank");
		
		if(isMysqlJdbcUrl(url)) {
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
		}else if(url.contains("jdbc:hsqldb:")) {
			return JdbcDataTypeUtil.getHSQLDataType(value);			
		}else {
			throw new UnsupportedOperationException("cannot get database type by url:"+url);
		}
	}

	public static boolean isMysqlJdbcUrl(String url) {
		return url.contains("mysql") || url.contains("mariadb");
	}
	
    public static String getClickHouseDataType(Object value) {
        if (value == null) {
            return "String";
        }

        if (value instanceof String) {
            return "String";
        }

        if(isDoubleNumber(value)) return "Float64";
		
		if(value instanceof Number || value instanceof Boolean) return "Int64";
		if(value instanceof Date) return "DateTime64(3)";

        return "String";
    }
    
    public static String getMysqlDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "DOUBLE";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "DATETIME";

        return VARCHAR;
    }
    
    public static String getOracleDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "NUMBER(38,4)";
		
		if(value instanceof Number || value instanceof Boolean) return "NUMBER(38,0)";
		if(value instanceof Date) return "DATE";

        return VARCHAR;
    }
    
    public static String getSqlServerDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "FLOAT";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "DATETIME";

        return VARCHAR;
    }
    
    public static String getPostgreSQLDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "float8";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "timestamp";

        return VARCHAR;
    }

	public static String getHiveDataType(Object value) {
        if (value == null) {
            return "STRING";
        }

        if (value instanceof String) {
            return "STRING";
        }

        if(isDoubleNumber(value)) return "DOUBLE";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "TIMESTAMP";

        return "STRING";
	}    
    
	public static String getH2DataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "NUMERIC(20, 2)";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "TIMESTAMP";

        return VARCHAR;		
	}

	public static String getHSQLDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "REAL";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "DATETIME";

        return VARCHAR;		
	}
	
	public static String getSybaseDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

		if(isDoubleNumber(value)) return "REAL";
		
		if(value instanceof Number || value instanceof Boolean) return "BIGINT";
		if(value instanceof Date) return "datetime";

        return VARCHAR;		
	}
	
	private static boolean isDoubleNumber(Object value) {
		if(value == null) return false;
		
		if(value instanceof Double) return true;
		if(value instanceof Float) return true;
		if(value.getClass() == double.class) return true;
		if(value.getClass() == float.class) return true;
		
		return false;
	}
}
