package com.github.dataswitch.output;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.github.dataswitch.support.DataSourceProvider;
import com.github.dataswitch.util.JdbcUtil;
import com.github.dataswitch.util.MapUtil;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.dataswitch.util.ParsedSql;
import com.github.dataswitch.util.Util;
import com.github.rapid.common.beanutils.PropertyUtils;


public class JdbcOutput extends DataSourceProvider implements Output {
	private static final String SQL_SEPARATOR_CHAR = ";";
	
	private static Logger logger = LoggerFactory.getLogger(JdbcOutput.class);
	private String lockSql;
	private String sql;
	private String beforeSql;
	private String afterSql;
	
	/**
	 * 要插入数据的表
	 */
	private String table;
	
	/**
	 * 自动增加列
	 */
	private boolean autoAlterTableAddColumn = false;
	
	/**
	 * 自动创建表
	 */
	private boolean autoCreateTable = false;
	
	/**
	 * 是否将命名参数替换成实际值
	 */
	private boolean replaceSqlWithParams = false;
	
	private transient TransactionTemplate transactionTemplate;
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
	
	public String getLockSql() {
		return lockSql;
	}

	public void setLockSql(String lockSql) {
		this.lockSql = lockSql;
	}

	public String getBeforeSql() {
		return beforeSql;
	}

	public void setBeforeSql(String beforeSql) {
		this.beforeSql = beforeSql;
	}

	public String getAfterSql() {
		return afterSql;
	}

	public void setAfterSql(String afterSql) {
		this.afterSql = afterSql;
	}
	
	public boolean isReplaceSqlWithParams() {
		return replaceSqlWithParams;
	}

	public void setReplaceSqlWithParams(boolean replaceSqlWithParams) {
		this.replaceSqlWithParams = replaceSqlWithParams;
	}
	
	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setAutoAlterTableAddColumn(boolean autoAlterTableAddColumn) {
		this.autoAlterTableAddColumn = autoAlterTableAddColumn;
	}

	public void setAutoCreateTable(boolean autoCreateTable) {
		this.autoCreateTable = autoCreateTable;
	}

	public void init() {
		executeWithSemicolonComma(getDataSource(),beforeSql);
		logger.info("executed beforeSql:"+beforeSql);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output.super.open(params);
		init();
	}
	
	private String getRealSql(final List<Object> rows) {
		String sql = getSql();
		if(StringUtils.isNotBlank(sql)) {
			return sql;
		}
		
		Assert.hasText(table,"table or sql must be not blank");
		
		Map allMap = MapUtil.mergeAllMap((List) rows);
		if(autoAlterTableAddColumn) {
			alterTableIfColumnMiss(new JdbcTemplate(getDataSource()), allMap,table);
		}
		
		sql = generateInsertSql2ByColumns(table, allMap);
		
		return sql;
	}
	
	private String generateInsertSql2ByColumns(String table, Map allMap) {
		return JdbcUtil.generateInsertSqlByColumns(table,new ArrayList(allMap.keySet()));
	}

	private void alterTableIfColumnMiss(JdbcTemplate jdbcTemplate, Map allMap, String table) {
		Map missColumns = getMissColumns(jdbcTemplate, allMap, table);
        if (missColumns == null) return;
        
        missColumns.forEach((key, value) -> {
        	String sql = "ALTER TABLE "+table+"  ADD COLUMN `"+key+"` "+getDatabaseDataType(value);
        	jdbcTemplate.execute(sql);
        });
        
        String cacheKey = JdbcUtil.getTableCacheKey(table, getJdbcUrl());
        JdbcUtil.tableColumnsCache.remove(cacheKey);
	}

	String _jdbcUrl = null;
	private String getDatabaseDataType(Object value) {
		String url = getJdbcUrl();
		
		if(url.contains("mysql") || url.contains("mariadb")) {
			return JdbcUtil.getMysqlDataType(value);
		}else if(url.contains("clickhouse")) {
			return JdbcUtil.getClickHouseDataType(value);
		}else if(url.contains("sqlserver")) {
			return JdbcUtil.getSqlServerDataType(value);
		}else if(url.contains("oracle")) {
			return JdbcUtil.getOracleDataType(value);	
		}else if(url.contains("postgresql")) {
			return JdbcUtil.getPostgreSQLDataType(value);	
		}else if(url.contains("hive2")) {
			return JdbcUtil.getHiveDataType(value);
		}else {
			throw new UnsupportedOperationException("cannot get database type by url:"+url);
		}
	}

	private String getJdbcUrl()  {
		if(StringUtils.isBlank(_jdbcUrl)) {
			_jdbcUrl = getUrl();
		}
		
		if(StringUtils.isBlank(_jdbcUrl)) {
			try {
				Connection connection = null;
				try {
					connection = getDataSource().getConnection();
					_jdbcUrl = connection.getMetaData().getURL();
				}finally {
					if(connection != null) {
						connection.close();
					}
				}
			}catch(Exception e) {
				throw new RuntimeException("cannot get jdbc url by jdbc connection",e);
			}
		}
		return _jdbcUrl;
	}

    
	private Map getMissColumns(JdbcTemplate jdbcTemplate, Map allMap, String table) {
        Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table,getJdbcUrl());
        return MapUtil.getDifferenceMap(tableColumns, allMap);
	}

	@Override
	public void write(final List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		long start = System.currentTimeMillis();
		String sql = executeWithJdbc(rows);
		long costTime = System.currentTimeMillis() - start;
		long tps = Util.getTPS(rows.size(), costTime);
		logger.info("execute update sql with rows:"+rows.size()+" costTimeMills:"+costTime+" tps:"+ tps +" for sql:"+sql);
	}

	protected String executeWithJdbc(final List<Object> rows) {
		String realSql = getRealSql(rows);
		
		if(replaceSqlWithParams) {
			ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(realSql);
			for(Object row : rows) {
				execWithReplacedSql(parsedSql, row);
			}
		}else {
			
			final String[] sqlArray = StringUtils.split(realSql,SQL_SEPARATOR_CHAR);
			TransactionTemplate tt = getTransactionTemplate();
			
			tt.execute(new TransactionCallback<Object>() {
				public Object doInTransaction(TransactionStatus status) {
					executeWithSemicolonComma(getDataSource(),lockSql);
					
					for(final String updateSql : sqlArray) {
						if(StringUtils.isBlank(updateSql)) 
							continue;
						SqlParameterSource[] batchArgs = newSqlParameterSource(rows);
						new NamedParameterJdbcTemplate(getDataSource()).batchUpdate(updateSql, batchArgs);
					}
					return true;
				}
			});
		}
		
		return realSql;
	}

	private void execWithReplacedSql(ParsedSql parsedSql, Object row) {
		String replacedSql = getReplacedSql(parsedSql, row);
//		new NamedParameterJdbcTemplate(getDataSource()).execute(replacedSql, paramMap, action)
		new JdbcTemplate(getDataSource()).execute(replacedSql);
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

	public TransactionTemplate getTransactionTemplate() {
		if(transactionTemplate == null) {
			transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(getDataSource()));
//			transactionTemplate.setIsolationLevelName("ISOLATION_READ_UNCOMMITTED");
		}
		return transactionTemplate;
	}
	
	public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
		this.transactionTemplate = transactionTemplate;
	}

	protected SqlParameterSource[] newSqlParameterSource(final List<Object> rows) {
		SqlParameterSource[] batchArgs = new SqlParameterSource[rows.size()];
		int i = 0;
		for (Object row : rows) {
			if(row instanceof Map) {
				batchArgs[i] = new MapSqlParameterSource((Map)row);
			}else {
				batchArgs[i] = new BeanPropertySqlParameterSource(row);
			}
			i++;
		}
		return batchArgs;
	}
	
	protected static void executeWithSemicolonComma(DataSource ds, String sql) {
		if (StringUtils.isBlank(sql)) {
			return;
		}
		
		final String[] sqls = StringUtils.split(sql,SQL_SEPARATOR_CHAR);;
		for (String s : sqls) {
			if(StringUtils.isBlank(s)) {
				continue;
			}
			
			new JdbcTemplate(ds).execute(s);
		}
	}
	
	@Override
	public void close() {
		DataSource dataSource = getDataSource();
		executeWithSemicolonComma(dataSource,afterSql);
		logger.info(" executed afterSql:"+afterSql);
	}
	
}
