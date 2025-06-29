package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;


public class JdbcSqlUtil {

	private static Logger logger = LoggerFactory.getLogger(JdbcSqlUtil.class);
	
	public static void update(DataSource ds,String tableName,List<Map> rows,String... primaryKeys) {
		Assert.notNull(ds,"dataSource must be not null");
		Assert.hasText(tableName,"tableName must be not empty");
		Assert.notEmpty(primaryKeys,"primaryKeys must be not empty");
		JdbcTemplate jt = new JdbcTemplate(ds);
		
		final NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(ds);
		List<Exception> exceptions = new ArrayList<Exception>();
		long startTime = System.currentTimeMillis();
		for(Map row : rows) {
			String updateSql = buildUpdateSql(tableName,row.keySet(),primaryKeys);
			try {
				namedJdbcTemplate.update(updateSql, row);
			}catch(Exception e) {
				logger.error("error on insert row:"+row,e);
				exceptions.add(new RuntimeException("error on row:"+row+" msg:"+e,e));
			}
		}
		long costTime = System.currentTimeMillis() - startTime;
		
		logger.info("update_table:"+tableName+" rows="+rows.size()+" costSeconds:"+(costTime / 1000)+" tps:"+(rows.size() * 1000.0 / costTime));
		
		if(CollectionUtils.isNotEmpty(exceptions)) {
			throw new RuntimeException("JdbcSqlUtil update error,exception_count:"+exceptions.size()+" exceptions:"+exceptions.toString());
		}
	}

	public static String buildUpdateSql(String tableName, Collection<String> columns,String... primaryKeys) {
		Assert.notEmpty(primaryKeys,"primaryKeys must be not empty");
		Assert.notEmpty(columns,"columns must be not empty");
		
		StringBuilder sql = new StringBuilder("UPDATE "+tableName+" SET ");
		
		boolean first = true;
		for(Object key : columns) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			sql.append("`"+key+"`");
			sql.append("=:"+key);
		}
		sql.append(" WHERE ");
		
		first = true;
		for(String primaryKey : primaryKeys) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			sql.append(primaryKey);
			sql.append("=:"+primaryKey);
		}
		return sql.toString();
	}
	
	public static String buildDeleteSql(String tableName, String... primaryKeys) {
		Assert.notEmpty(primaryKeys,"primaryKeys must be not empty");
		
		StringBuilder sql = new StringBuilder("DELETE FROM "+tableName+" WHERE ");
		
		boolean first = true;
		for(String primaryKey : primaryKeys) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			sql.append(primaryKey);
			sql.append("=:"+primaryKey);
		}
		return sql.toString();
	}
	
	public static String buildInsertSql(String table, Collection<String> columns) {
		if(CollectionUtils.isEmpty(columns))
			return null;
		
		StringJoiner valueJoiner = new StringJoiner(",");
		StringJoiner keyJoiner = new StringJoiner(",");
		columns.forEach((columnName) -> {
			if(StringUtils.isBlank(columnName)) return;
			
			valueJoiner.add(":"+columnName);
//			keyJoiner.add("`"+columnName+"`");
			keyJoiner.add(columnName);
		});
		
		String sql = "INSERT INTO " + table + " ("+keyJoiner.toString()+") VALUES ("+valueJoiner.toString()+")";
		return sql;
	}
	
	
	public static String buildMysqlInsertOrUpdateSql(String tableName,Collection<String> columns,String... primaryKeys) {
		Assert.notEmpty(primaryKeys,"primaryKeys must be not empty");
		
		StringBuilder sql = new StringBuilder(buildInsertSql(tableName, columns));
		appendMysqlOnDuplicateKeyUpdate(sql,columns,primaryKeys);
		return sql.toString();
	}
	
	private static void appendMysqlOnDuplicateKeyUpdate(StringBuilder sql,Collection<String> columns, String[] primaryKeys) {
		if(ArrayUtils.isEmpty(primaryKeys))
			return;
		
		sql.append(" ON DUPLICATE KEY UPDATE ");
			
		assertPrimaryKeysContains(columns, primaryKeys);
		
		boolean first = true;
		for(String c : columns) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
			}
			sql.append("`"+c+"`=").append("values(`"+c+"`)");
		}
	}

	private static void assertPrimaryKeysContains(Collection<String> columns, String[] primaryKeys) {
		Set<String> valueColumns = new HashSet(columns);
		for(String key : primaryKeys) {
			if(!valueColumns.remove(key)) {
				throw new RuntimeException("not found primary key:"+key+" in columns:"+columns);
			}
		}
	}


	
}
