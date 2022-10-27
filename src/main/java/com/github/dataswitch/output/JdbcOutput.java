package com.github.dataswitch.output;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.support.DataSourceProvider;
import com.github.dataswitch.util.DefaultValueMapSqlParameterSource;
import com.github.dataswitch.util.JdbcCreateTableSqlUtil;
import com.github.dataswitch.util.JdbcDataTypeUtil;
import com.github.dataswitch.util.JdbcSqlUtil;
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
	private String outputMode = OutputMode.insert.name(); //insert/replace/update, 控制写入数据到目标表采用 insert into 或者 replace into 或者 ON DUPLICATE KEY UPDATE 语句

	
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
	 * 是否已经执行过create table sql
	 */
	private boolean _executedCreateTable = false;
	
	/**
	 * 是否将命名参数替换成实际值
	 */
	private boolean replaceSqlWithParams = false;
	
	private transient TransactionTemplate transactionTemplate;
	
	private String primaryKeys; //主键字段
	
	private int batchSize = Constants.DEFAULT_BUFFER_SIZE; //批量大小
	
	private String columnsFrom = ColumnsFrom.input.name(); //输入列来源: table or input or config
	private String columns; //要更新的列
	
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
	
	public String getSessionSql() {
		return sessionSql;
	}

	public void setSessionSql(String sessionSql) {
		this.sessionSql = sessionSql;
	}

	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(String primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public void setAutoAlterTableAddColumn(boolean autoAlterTableAddColumn) {
		this.autoAlterTableAddColumn = autoAlterTableAddColumn;
	}

	public void setAutoCreateTable(boolean autoCreateTable) {
		this.autoCreateTable = autoCreateTable;
	}
	
	public String getOutputMode() {
		return outputMode;
	}

	public void setOutputMode(String outputMode) {
		this.outputMode = outputMode;
	}
	
	public void outputMode(OutputMode outputMode) {
		this.outputMode = outputMode.name();
	}
	
	public void setColumnsFrom(String columnsFrom) {
		this.columnsFrom = columnsFrom;
	}
	
	public void columnsFrom(ColumnsFrom columnsFrom) {
		this.columnsFrom = columnsFrom.name();
	}

	public void setColumns(String columns) {
		this.columns = columns;
		if(StringUtils.isNotBlank(columns)) {
			columnsFrom(ColumnsFrom.config);
		}
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
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
	
	private String alterTableAndGetFinalSql(final List<Object> rows) {
		String sql = getSql();
		if(StringUtils.isNotBlank(sql)) {
			return sql;
		}
		
		Assert.hasText(table,"table or sql must be not blank");
		
		JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());
		executeWithSemicolonComma(getDataSource(), sessionSql);

		Map allColumnsWithValue = MapUtil.mergeAllMapWithNotNullValue((List) rows);
		
		Map<String,String> columnsSqlType = JdbcDataTypeUtil.getDatabaseDataType(cacheJdbcUrl(), allColumnsWithValue);
		if(autoCreateTable) {
			executeCreateTableSql(jdbcTemplate,columnsSqlType);
		}
		
		Set<String> tableColumnNames = columnsSqlType.keySet();
		
		if(autoAlterTableAddColumn) {
			JdbcUtil.alterTableIfColumnMiss(jdbcTemplate, allColumnsWithValue,table,cacheJdbcUrl());
		}
		
		return generateSql(jdbcTemplate, tableColumnNames);
	}

	private String generateSql(JdbcTemplate jdbcTemplate, Set<String> tableColumnNames) {
		Collection<String> columns = getColumnsByColumnsFrom(jdbcTemplate,tableColumnNames);
		Assert.notEmpty(columns,"columns must be not empty");
		
		String sql = null;
		if(OutputMode.insert.name().equals(outputMode)) {
			sql = JdbcSqlUtil.buildInsertSql(table, columns);
		}else if(OutputMode.replace.name().equals(outputMode)) {
			sql = JdbcSqlUtil.buildMysqlInsertOrUpdateSql(table, columns, getPrimaryKeysArray());
		}else if(OutputMode.update.name().equals(outputMode)) {
			sql = JdbcSqlUtil.buildUpdateSql(table, columns, getPrimaryKeysArray());
		}else {
			throw new UnsupportedOperationException("error outputMode:"+outputMode);
		}
		return sql;
	}

	private Collection<String> getColumnsByColumnsFrom(JdbcTemplate jdbcTemplate,Set<String> tableColumnNames) {
		if(ColumnsFrom.input.name().equals(columnsFrom)) {
			return tableColumnNames;
		}else if(ColumnsFrom.table.name().equals(columnsFrom)) {
			return JdbcUtil.getTableColumnsName(jdbcTemplate, table, cacheJdbcUrl());
		}else if(ColumnsFrom.config.name().equals(columnsFrom)) {
			return Arrays.asList(JdbcUtil.splitTableColumns(columns));
		}else {
			throw new UnsupportedOperationException("error ColumnsFrom:" + columnsFrom);
		}
	}

	private void executeCreateTableSql(JdbcTemplate jdbcTemplate,Map<String,String> columnsSqlType) {
		if(_executedCreateTable) return;
		
		String createTableSql = JdbcCreateTableSqlUtil.buildCreateTableSql(table, null, columnsSqlType, getPrimaryKeysArray());
		
		try {
			if(StringUtils.isNotBlank(createTableSql)) {
				if(!JdbcUtil.tableExists(jdbcTemplate,table,cacheJdbcUrl())) {
					jdbcTemplate.execute(createTableSql);
					logger.info("executeCreateTableSql() "+ createTableSql);
				}
			}
		}catch(Exception e) {
			logger.warn("execute create table sql error:"+createTableSql,e);
		}
		
		_executedCreateTable = true;
	}
	
	private String[] _primaryKeyArray;
	private String[] getPrimaryKeysArray() {
		if(_primaryKeyArray == null) {
			if(StringUtils.isBlank(primaryKeys)) {
				List<String> tablePrimaryKeysList = JdbcUtil.getTablePrimaryKeysList(table,new JdbcTemplate(getDataSource()));
				primaryKeys = StringUtils.join(tablePrimaryKeysList,",");
				logger.info("get primary key:["+primaryKeys +"] from database metadata for table:"+table);
			}
			
			_primaryKeyArray = JdbcUtil.splitTableColumns(primaryKeys);
			
			Assert.notEmpty(_primaryKeyArray,"not found primary key on table:"+table+",your can config by:primaryKeys");
		}
		return _primaryKeyArray;
	}


	@Override
	public void write(final List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		long start = System.currentTimeMillis();
		String finalSql = executeWithJdbc(rows);
		long costTime = System.currentTimeMillis() - start;
		long tps = Util.getTPS(rows.size(), costTime);
		logger.info("execute update sql with rows:"+rows.size()+" costTimeMills:"+costTime+" tps:"+ tps +" for sql:"+finalSql);
	}

	protected String executeWithJdbc(final List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return null;
		
		String finalSql = alterTableAndGetFinalSql(rows);
		
		if(replaceSqlWithParams) {
			ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(finalSql);
			for(Object row : rows) {
				execWithReplacedSql(parsedSql, row);
			}
		}else {
			
			final String[] updateSqls = StringUtils.split(finalSql,SQL_SEPARATOR_CHAR);
			TransactionTemplate tt = getTransactionTemplate();
			
			tt.execute(new TransactionCallback<Object>() {
				public Object doInTransaction(TransactionStatus status) {
					executeWithSemicolonComma(getDataSource(),lockSql);
					
					SqlParameterSource[] batchArgs = newSqlParameterSource(rows);
					
					for(final String updateSql : updateSqls) {
						if(StringUtils.isBlank(updateSql)) 
							continue;
						
						try {
							new NamedParameterJdbcTemplate(getDataSource()).batchUpdate(updateSql, batchArgs);
						}catch(Exception e) {
							throw new RuntimeException("execute sql error,sql:"+updateSql+" firstRow:"+rows.get(0),e);
						}
					}
					return true;
				}
			});
		}
		
		return finalSql;
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
	
	public static enum OutputMode {
		insert,update,replace
	}
	
	//列的来源
	public static enum ColumnsFrom {
		table, //来自表
		input, //来自输入数据
		config //来自设置属性
	}
	
}
