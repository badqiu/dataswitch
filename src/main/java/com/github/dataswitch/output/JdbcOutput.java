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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.github.dataswitch.support.DataSourceProvider;
import com.github.dataswitch.util.DefaultValueMapSqlParameterSource;
import com.github.dataswitch.util.JdbcDataTypeUtil;
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
		if(StringUtils.isBlank(sql) && StringUtils.isBlank(table)) {
			throw new IllegalStateException("table or sql must be not blank");
		}
		
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
		JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());
		if(autoAlterTableAddColumn) {
			alterTableIfColumnMiss(jdbcTemplate, allMap,table);
			sql = generateInsertSql2ByColumns(table, allMap);
		}else {
			sql = generateInsertSqlByTargetTable(jdbcTemplate,table);
		}
		
		return sql;
	}
	
	private String generateInsertSqlByTargetTable(JdbcTemplate jdbcTemplate,String table) {
		Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table, getJdbcUrl());
		ArrayList columns = new ArrayList(MapUtil.keyToLowerCase(tableColumns).keySet());
		return JdbcUtil.generateInsertSqlByColumns(table,columns);
	}

	private String generateInsertSql2ByColumns(String table, Map allMap) {
		return JdbcUtil.generateInsertSqlByColumns(table,new ArrayList(allMap.keySet()));
	}

	private void alterTableIfColumnMiss(JdbcTemplate jdbcTemplate, Map allMap, String table) {
		Map missColumns = getMissColumns(jdbcTemplate, allMap, table);
        if (missColumns == null) return;
        
        missColumns.forEach((key, value) -> {
        	long start = System.currentTimeMillis();
        	String sql = "ALTER TABLE `"+table+"`  ADD COLUMN `"+key+"` "+getDatabaseDataType(value);
        	jdbcTemplate.execute(sql);
        	long cost = start - System.currentTimeMillis();
        	logger.info("executed alter_table_add_column sql:["+sql+"], costSeconds:"+(cost/1000));
        });
        
        String cacheKey = JdbcUtil.getTableCacheKey(table, getJdbcUrl());
        JdbcUtil.tableColumnsCache.remove(cacheKey);
	}

	String _cacheJdbcUrl = null;
	public String getDatabaseDataType(Object value) {
		String url = getJdbcUrl();
		return JdbcDataTypeUtil.getDatabaseDataType(url, value);
	}

	private String getJdbcUrl()  {
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			_cacheJdbcUrl = getUrl();
		}
		
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			try {
				Connection connection = null;
				try {
					connection = getDataSource().getConnection();
					_cacheJdbcUrl = connection.getMetaData().getURL();
				}finally {
					if(connection != null) {
						connection.close();
					}
				}
			}catch(Exception e) {
				throw new RuntimeException("cannot get jdbc url by jdbc connection",e);
			}
		}
		
		return _cacheJdbcUrl;
	}

    
	private Map getMissColumns(JdbcTemplate jdbcTemplate, Map allMap, String table) {
        Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table,getJdbcUrl());
        return MapUtil.getDifferenceMap(MapUtil.keyToLowerCase(tableColumns), MapUtil.keyToLowerCase(allMap));
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
		if(CollectionUtils.isEmpty(rows)) return null;
		
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
						
						try {
							SqlParameterSource[] batchArgs = newSqlParameterSource(rows);
							new NamedParameterJdbcTemplate(getDataSource()).batchUpdate(updateSql, batchArgs);
						}catch(Exception e) {
							throw new RuntimeException("execute sql error,sql:"+updateSql+" firstRow:"+rows.get(0),e);
						}
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
				Map rowMap = (Map)row;
				Object defaultValue = null;
				DefaultValueMapSqlParameterSource defaultValueMapSqlParameterSource = new DefaultValueMapSqlParameterSource(rowMap);
				defaultValueMapSqlParameterSource.setDefaultValue(defaultValue);
				
				batchArgs[i] = defaultValueMapSqlParameterSource;
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
