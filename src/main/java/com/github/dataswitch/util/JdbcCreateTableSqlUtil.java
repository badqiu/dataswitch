package com.github.dataswitch.util;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

public class JdbcCreateTableSqlUtil {
	
	private static Logger logger = LoggerFactory.getLogger(JdbcCreateTableSqlUtil.class);
	
	public static void executeCreateTableSql(String tableName,Map columnComments,Map<String, String> columnsSqlType,JdbcTemplate jt,String[] primaryKeys) {
		String createTableSql = buildCreateTableSql(tableName,columnComments, columnsSqlType,primaryKeys);
		
		if(StringUtils.isNotBlank(createTableSql)) {
			jt.execute(createTableSql);
			logger.info("executeCreateTableSql() "+ createTableSql);
		}
	}
	
	public static String buildCreateTableSql(String tableName,Map columnComments, Map<String, String> columnsSqlType,String[] primaryKeys) {
		if(columnsSqlType == null || columnsSqlType.isEmpty()) return null;
		
		Assert.notEmpty(primaryKeys,"primary keys must be not empty");
		
		StringBuilder sql = new StringBuilder(" create table  "+tableName+" (");
		boolean first = true;
		for(Map.Entry<String, String> entry : columnsSqlType.entrySet()) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			String key = entry.getKey();
			String sqlType = entry.getValue();
			
			//缩短主键长度，避免mysql主键过长报错
			if(ArrayUtils.contains(primaryKeys, key) && sqlType.contains("varchar")) {
				sqlType = "varchar(50)";			
			}
			
			String comment = getComment(key,columnComments);
			
			sql.append("`"+key + "` " +sqlType +comment);
		}
		
		addPrimaryKeys(sql, primaryKeys);
		
		return sql.append(" ) ").toString();
		
	}

	private static String getComment(String key, Map columnComments) {
		if(columnComments == null) return "";
		
		Object com = columnComments.get(key);
		if(com == null || StringUtils.isBlank(com.toString())) {
			return "";
		}
		return " comment '"+com+"'";
	}

	private static void addPrimaryKeys(StringBuilder sql, String[] primaryKeys) {
		if(ArrayUtils.isEmpty(primaryKeys)) {
			return;
		}
		
		boolean first;
		first = true;
		sql.append(", primary key(");
		for(String key : primaryKeys) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			sql.append(key);
		}
		sql.append(" ) ");
	}
	
}
