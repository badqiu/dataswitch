package com.github.dataswitch.util;


import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
/**
 * 
 * @author badqiu
 */
public class JavaSqlTypesUtil {

	private final static Map<Integer,Class> _preferredJavaTypeForSqlType = new HashMap<Integer,Class>();
	 
	public static boolean isFloatNumber(int sqlType,int size,int decimalDigits) {
		String javaType = getPreferredJavaTypeString(sqlType,size,decimalDigits);
		if(javaType.endsWith("Float") || javaType.endsWith("Double") || javaType.endsWith("BigDecimal")) {
			return true;
		}
		return false;
	}
	
	public static boolean isIntegerNumber(int sqlType,int size,int decimalDigits) {
		String javaType = getPreferredJavaTypeString(sqlType,size,decimalDigits);
		if(javaType.endsWith("Long") || javaType.endsWith("Integer") || javaType.endsWith("Short")) {
			return true;
		}
		return false;
	}

	public static boolean isDate(int sqlType,int size,int decimalDigits) {
		String javaType = getPreferredJavaTypeString(sqlType,size,decimalDigits);
		if(javaType.endsWith("Date") || javaType.endsWith("Timestamp") || javaType.endsWith("Time")) {
			return true;
		}
		return false;
	}
	
	public static Object getPreferredJavaTypeValue(int sqlType, int size,
			int decimalDigits) {
		Class clazz = getPreferredJavaType(sqlType,size,decimalDigits);
		if(JdbcDataTypeUtil.isDoubleNumber(clazz)) {
			return 0.0d;
		}
		if(JdbcDataTypeUtil.isDateType(clazz)) {
			return new Date(1);
		}
		if(JdbcDataTypeUtil.isIntegerNumber(clazz)) {
			return 0;
		}
		if(clazz == String.class) {
			return "";
		}
		if(clazz.isArray()) {
			return new Object[0];
		}
		
		return "";
	}
	
	public static String getPreferredJavaTypeString(int sqlType, int size,
			int decimalDigits) {
		return getPreferredJavaType(sqlType,size,decimalDigits).getName();
	}
	
	public static Class getPreferredJavaType(int sqlType, int size,
			int decimalDigits) {
		if ((sqlType == Types.DECIMAL || sqlType == Types.NUMERIC)
				&& decimalDigits == 0) {
			if (size == 1) {
				// https://sourceforge.net/tracker/?func=detail&atid=415993&aid=662953&group_id=36044
				return java.lang.Boolean.class;
			} else if (size < 3) {
				return java.lang.Byte.class;
			} else if (size < 5) {
				return java.lang.Short.class;
			} else if (size < 10) {
				return java.lang.Integer.class;
			} else if (size < 19) {
				return java.lang.Long.class;
			} else {
				return java.math.BigDecimal.class;
			}
		}
		
		Class result = _preferredJavaTypeForSqlType.get(sqlType);
		if (result == null) {
			result = Object.class;
		}
		return result;
	}
		   
   static {
	  //integer
      _preferredJavaTypeForSqlType.put(Types.TINYINT, java.lang.Byte.class);
      _preferredJavaTypeForSqlType.put(Types.SMALLINT, java.lang.Short.class);
      _preferredJavaTypeForSqlType.put(Types.INTEGER, java.lang.Integer.class);
      _preferredJavaTypeForSqlType.put(Types.BIGINT, java.lang.Long.class);
      _preferredJavaTypeForSqlType.put(Types.BIT, java.lang.Boolean.class);
      
      //double
      _preferredJavaTypeForSqlType.put(Types.REAL, java.lang.Float.class);
      _preferredJavaTypeForSqlType.put(Types.FLOAT, java.lang.Double.class);
      _preferredJavaTypeForSqlType.put(Types.DOUBLE, java.lang.Double.class);
      _preferredJavaTypeForSqlType.put(Types.DECIMAL, java.math.BigDecimal.class);
      _preferredJavaTypeForSqlType.put(Types.NUMERIC, java.math.BigDecimal.class);
      
      //String
      _preferredJavaTypeForSqlType.put(Types.CHAR, java.lang.String.class);
      _preferredJavaTypeForSqlType.put(Types.VARCHAR, java.lang.String.class);
      // according to resultset.gif, we should use java.io.Reader, but String is more convenient for EJB
      _preferredJavaTypeForSqlType.put(Types.LONGVARCHAR, java.lang.String.class);
      _preferredJavaTypeForSqlType.put(Types.NVARCHAR, java.lang.String.class);
      _preferredJavaTypeForSqlType.put(Types.LONGNVARCHAR, java.lang.String.class);
      
      //Date
      _preferredJavaTypeForSqlType.put(Types.DATE, java.sql.Date.class);
      _preferredJavaTypeForSqlType.put(Types.TIME, java.sql.Time.class);
      _preferredJavaTypeForSqlType.put(Types.TIMESTAMP, java.sql.Timestamp.class);
      _preferredJavaTypeForSqlType.put(Types.TIME_WITH_TIMEZONE, java.sql.Time.class);
      _preferredJavaTypeForSqlType.put(Types.TIMESTAMP_WITH_TIMEZONE, java.sql.Timestamp.class);
      
      //byte[]
      _preferredJavaTypeForSqlType.put(Types.BINARY, byte[].class);
      _preferredJavaTypeForSqlType.put(Types.VARBINARY, byte[].class);
      _preferredJavaTypeForSqlType.put(Types.LONGVARBINARY, java.io.InputStream.class);
      
      //other
      _preferredJavaTypeForSqlType.put(Types.CLOB, java.sql.Clob.class);
      _preferredJavaTypeForSqlType.put(Types.BLOB, java.sql.Blob.class);
      _preferredJavaTypeForSqlType.put(Types.ARRAY, java.sql.Array.class);
      _preferredJavaTypeForSqlType.put(Types.REF, java.sql.Ref.class);
      _preferredJavaTypeForSqlType.put(Types.STRUCT, java.sql.Struct.class);
      _preferredJavaTypeForSqlType.put(Types.JAVA_OBJECT, java.lang.Object.class);
   }
		

	   
}
