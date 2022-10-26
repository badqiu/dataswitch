package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
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
import com.github.dataswitch.util.JdbcUtil;
import com.github.dataswitch.util.MapUtil;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.dataswitch.util.ParsedSql;
import com.github.dataswitch.util.Util;


public class JdbcOutput extends DataSourceProvider implements Output {
	private static final String SQL_SEPARATOR_CHAR = ";";
	
	private static Logger logger = LoggerFactory.getLogger(JdbcOutput.class);
	private String lockSql;
	private String sql;
	private String beforeSql;
	private String afterSql;

	private String sessionSql; //在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性
	private String outputMode = "insert"; //insert/replace/update, 控制写入数据到目标表采用 insert into 或者 replace into 或者 ON DUPLICATE KEY UPDATE 语句

	
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
	
	private String getRealSqlOrAlterTable(final List<Object> rows) {
		String sql = getSql();
		if(StringUtils.isNotBlank(sql)) {
			return sql;
		}
		
		Assert.hasText(table,"table or sql must be not blank");
		
		Map allMap = MapUtil.mergeAllMap((List) rows);
		
		JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());
		executeWithSemicolonComma(getDataSource(), sessionSql);
		
		if(autoAlterTableAddColumn) {
			JdbcUtil.alterTableIfColumnMiss(jdbcTemplate, allMap,table,cacheJdbcUrl());
			sql = generateInsertSql2ByColumns(table, allMap);
		}else {
			
			if("insert".equals(outputMode)) {
				sql = generateInsertSqlByTargetTable(jdbcTemplate,table);
				setSql(sql);
			}else if("replace".equals(outputMode)) {
				throw new UnsupportedOperationException("error outputMode:"+outputMode);
			}else if("update".equals(outputMode)) {
				throw new UnsupportedOperationException("error outputMode:"+outputMode);
			}else {
				throw new UnsupportedOperationException("error outputMode:"+outputMode);
			}
			
		}
		
		return sql;
	}
	
	private String generateInsertSqlByTargetTable(JdbcTemplate jdbcTemplate,String table) {
		Map tableColumns = JdbcUtil.getTableColumns(jdbcTemplate, table, cacheJdbcUrl());
		ArrayList columns = new ArrayList(MapUtil.keyToLowerCase(tableColumns).keySet());
		return JdbcUtil.generateInsertSqlByColumns(table,columns);
	}

	private String generateInsertSql2ByColumns(String table, Map allMap) {
		ArrayList columns = new ArrayList(allMap.keySet());
		return JdbcUtil.generateInsertSqlByColumns(table,columns);
	}


	String _cacheJdbcUrl = null;
	private String cacheJdbcUrl()  {
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			_cacheJdbcUrl = getUrl();
		}
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			_cacheJdbcUrl = JdbcUtil.getJdbcUrl(getDataSource());
		}
		return _cacheJdbcUrl;
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
		
		String realSql = getRealSqlOrAlterTable(rows);
		
		if(replaceSqlWithParams) {
			ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(realSql);
			for(Object row : rows) {
				execWithReplacedSql(parsedSql, row);
			}
		}else {
			
			final String[] updateSqls = StringUtils.split(realSql,SQL_SEPARATOR_CHAR);
			TransactionTemplate tt = getTransactionTemplate();
			
			tt.execute(new TransactionCallback<Object>() {
				public Object doInTransaction(TransactionStatus status) {
					executeWithSemicolonComma(getDataSource(),lockSql);
					
					for(final String updateSql : updateSqls) {
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
		String replacedSql = JdbcUtil.getReplacedSql(parsedSql, row);
		new JdbcTemplate(getDataSource()).execute(replacedSql);
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
		
		boolean isClickhouseDatabase = isClickhouseDatabase();
		for (Object row : rows) {
			if(row instanceof Map) {
				Map rowMap = (Map)row;
				Object defaultValue = isClickhouseDatabase ? "" : null; //clickhouse不能插入null值
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
	
	private boolean isClickhouseDatabase() {
		return cacheJdbcUrl().contains("clickhouse");
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
