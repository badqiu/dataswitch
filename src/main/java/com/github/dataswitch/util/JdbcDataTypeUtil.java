package com.github.dataswitch.util;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

public class JdbcDataTypeUtil {

	private static final String VARCHAR = "VARCHAR(4000)";
	private static final String VARCHAR2 = "VARCHAR2(4000)";
	
	/**
	 * 根据输入数据，输出数据类型
	 * @param jdbcUrl
	 * @param inputData
	 * @return
	 */
	public static Map<String,String> getDatabaseDataType(String jdbcUrl,Map<String,Object> inputData,Map<String,String> columnsSqlType,String defaultSqlType) {
		Map<String,String> result = new LinkedHashMap<String,String>();
		inputData.forEach((key,value) -> {
			String sqlType = columnsSqlType != null ? columnsSqlType.get(key) : null;
			
			if(StringUtils.isBlank(sqlType)) {
				sqlType = getDatabaseDataType(jdbcUrl,value,defaultSqlType);
			}
			result.put(key, sqlType);
		});
		return result;
	}
	
	/**
	 *  通过java类型猜测数据库数据类型
	 * @param jdbcUrl
	 * @param value
	 * @param defaultSqlType 默认的数据类型，如果为null值
	 * @return
	 */
	public static String getDatabaseDataType(String jdbcUrl,Object value,String defaultSqlType) {
		Assert.hasText(jdbcUrl,"jdbcUrl must be not blank");
		if(value == null && StringUtils.isNotBlank(defaultSqlType)) {
			return defaultSqlType;
		}
		
		if(isMysqlJdbcUrl(jdbcUrl)) {
			return JdbcDataTypeUtil.getMysqlDataType(value);
		}else if(jdbcUrl.contains("clickhouse")) {
			return JdbcDataTypeUtil.getClickHouseDataType(value);
		}else if(jdbcUrl.contains("sqlserver")) {
			return JdbcDataTypeUtil.getSqlServerDataType(value);
		}else if(jdbcUrl.contains("oracle")) {
			return JdbcDataTypeUtil.getOracleDataType(value);	
		}else if(jdbcUrl.contains("postgresql")) {
			return JdbcDataTypeUtil.getPostgreSQLDataType(value);	
		}else if(jdbcUrl.contains("hive2")) {
			return JdbcDataTypeUtil.getHiveDataType(value);
		}else if(jdbcUrl.contains("jdbc:h2:")) {
			return JdbcDataTypeUtil.getH2DataType(value);	
		}else if(jdbcUrl.contains("jdbc:hsqldb:")) {
			return JdbcDataTypeUtil.getHSQLDataType(value);	
		}else if(jdbcUrl.contains("jdbc:sqlite:")) {
			return JdbcDataTypeUtil.getSqlLiteDataType(value);			
		}else {
			throw new UnsupportedOperationException("cannot get database type by url:"+jdbcUrl);
		}
	}

	public static boolean isMysqlJdbcUrl(String jdbcUrl) {
		return jdbcUrl.contains("mysql") || jdbcUrl.contains("mariadb");
	}
	
    public static String getClickHouseDataType(Object value) {
        if (value == null) {
            return "String";
        }

        if (value instanceof String) {
            return "String";
        }

        if(isDoubleNumber(value)) return "Float64";
		
		if(isIntegerNumber(value)) return "Int64";
		if(isDateType(value)) return "DateTime64(3)";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "DATETIME";

        return VARCHAR;
    }
    
    public static String getOracleDataType(Object value) {
        if (value == null) {
            return VARCHAR2;
        }

        if (value instanceof String) {
            return VARCHAR2;
        }

        if(isDoubleNumber(value)) return "NUMBER(38,4)";
		
		if(isIntegerNumber(value)) return "NUMBER(38,0)";
		if(isDateType(value)) return "TIMESTAMP";

        return VARCHAR2;
    }
    
    public static String getSqlServerDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

        if(isDoubleNumber(value)) return "FLOAT";
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "DATETIME";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "timestamp";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "TIMESTAMP";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "TIMESTAMP";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "DATETIME";

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
		
		if(isIntegerNumber(value)) return "BIGINT";
		if(isDateType(value)) return "datetime";

        return VARCHAR;		
	}

	public static String getSqlLiteDataType(Object value) {
        if (value == null) {
            return VARCHAR;
        }

        if (value instanceof String) {
            return VARCHAR;
        }

		if(isDoubleNumber(value)) return "REAL";
		
		if(isIntegerNumber(value)) return "INTEGER";
		if(isDateType(value)) return "DATETIME";

        return VARCHAR;		
	}


	public static String getTiDBDataType(Object value) {
        return getMysqlDataType(value);	
	}
	
	public static boolean isIntegerNumber(Object value) {
		if(isDoubleNumber(value)) return false;
		
		if(value instanceof Long || value instanceof Integer) {
			return true;
		}
		if(value.getClass() == int.class) return true;
		if(value.getClass() == long.class) return true;
		
		return value instanceof Number || value instanceof Boolean;
	}
	
	public static boolean isDateType(Object value) {
		return value instanceof Date;
	}
	
	public static boolean isDoubleNumber(Object value) {
		if(value == null) return false;
		
		if(value instanceof Double) return true;
		if(value instanceof Float) return true;
		if(value.getClass() == double.class) return true;
		if(value.getClass() == float.class) return true;
		if(value.getClass() == BigDecimal.class) return true;
		
		return false;
	}
}
