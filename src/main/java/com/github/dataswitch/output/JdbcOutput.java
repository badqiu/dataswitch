package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.ColumnsFrom;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.support.DataSourceProvider;
import com.github.dataswitch.util.CollectionUtil;
import com.github.dataswitch.util.DefaultValueMapSqlParameterSource;
import com.github.dataswitch.util.JdbcCreateTableSqlUtil;
import com.github.dataswitch.util.JdbcDataTypeUtil;
import com.github.dataswitch.util.JdbcSqlUtil;
import com.github.dataswitch.util.JdbcUtil;
import com.github.dataswitch.util.MapUtil;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.dataswitch.util.ParsedSql;
import com.github.dataswitch.util.PropertiesUtil;
import com.github.dataswitch.util.Util;


public class JdbcOutput extends DataSourceProvider implements Output {
	
	
	private static Logger logger = LoggerFactory.getLogger(JdbcOutput.class);
	
	
	private String lockSql;
	private String sql; //要执行的update SQL,优先级高于table属性
	private String beforeSql; // open开始时执行的SQL
	private String afterSql; // close退出时执行的SQL

	private String sessionSql; //在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性
	private OutputMode outputMode = OutputMode.insert; //insert/replace/update, 控制写入数据到目标表采用 insert into 或者 replace into 或者 ON DUPLICATE KEY UPDATE 语句

	
	/**
	 * 表名，作用优先级低于sql属性
	 */
	private String table;
	
	/**
	 * 自动增加列
	 */
	private boolean autoAlterTableAddColumn = false;
	
	/**
	 * 自动增加列时，如果数值为null,默认的数据类型,示例值: BIGINT,VARCHAR(200)
	 */
	private String defaultColumnSqlType; 
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
	
	/**
	 * 主键字段,多列用逗号分隔
	 */
	private String primaryKeys; 
	
	/**
	 * 批量更新的数据大小
	 */
	private int batchSize = Constants.DEFAULT_BUFFER_SIZE;
	
	/**
	 * 输入列来源: table or input or config
	 */
	private ColumnsFrom columnsFrom = ColumnsFrom.input; 
	
	/**
	 * 要更新的列,多列用逗号分隔
	 */
	private String columns; 
	
	/**
	 * 列的sql类型
	 */
	private Map<String,String> columnsSqlType = new HashMap(); 
	/**
	 * 列的注释
	 */
	private Map<String,String> columnsComment = new HashMap(); 
	
	private FailMode failMode = FailMode.FAIL_FAST;
	
	private boolean renameTable = false;
	private String finalTable =  null; //重命名后，最终的表名
	
	private Exception exception = null;
	
	// 保留起来的所有数据列及值
	private Map<String,Object> _allColumnWithValue = new HashMap();
			
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
	
	public boolean isAutoAlterTableAddColumn() {
		return autoAlterTableAddColumn;
	}
	
	public String getDefaultColumnSqlType() {
		return defaultColumnSqlType;
	}

	public void setDefaultColumnSqlType(String defaultSqlType) {
		this.defaultColumnSqlType = defaultSqlType;
	}

	public Exception getException() {
		return exception;
	}

	public void setAutoCreateTable(boolean autoCreateTable) {
		this.autoCreateTable = autoCreateTable;
	}

	public boolean isAutoCreateTable() {
		return autoCreateTable;
	}

	public String getOutputMode() {
		return outputMode.name();
	}

	public void setOutputMode(String outputMode) {
		this.outputMode = OutputMode.valueOf(outputMode);
	}
	
	public void outputMode(OutputMode outputMode) {
		this.outputMode = outputMode;
	}
	
	public void setColumnsFrom(String columnsFrom) {
		this.columnsFrom = ColumnsFrom.valueOf(columnsFrom);
	}
	
	public void columnsFrom(ColumnsFrom columnsFrom) {
		this.columnsFrom = columnsFrom;
	}
	
	public void setColumnsSqlType(Map<String, String> columnsSqlType) {
		this.columnsSqlType = columnsSqlType;
	}

	public void setColumnsSqlTypeProperties(String columnsSqlType) {
		setColumnsSqlType((Map)PropertiesUtil.createProperties(columnsSqlType));
	}
	
	public Map<String, String> getColumnsSqlType() {
		return columnsSqlType;
	}
	
	public void setColumnsComment(Map<String, String> columnsComment) {
		this.columnsComment = columnsComment;
	}
	
	public Map<String, String> getColumnsComment() {
		return columnsComment;
	}
	
	public void setColumnsCommentProperties(String columnsComment) {
		setColumnsComment((Map)PropertiesUtil.createProperties(columnsComment));
	}

	public void setColumns(String columns) {
		this.columns = columns;
		if(StringUtils.isNotBlank(columns)) {
			columnsFrom(ColumnsFrom.config);
		}
	}
	
	public String getColumns() {
		return columns;
	}

	public void setBatchSize(int batchSize) {
		Assert.isTrue(batchSize > 0,"batchSize > 0 must be true");
		this.batchSize = batchSize;
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public String getFailMode() {
		return failMode.name();
	}
	
	public void setFailMode(String failMode) {
		this.failMode = FailMode.getRequiredByName(failMode);
	}
	
	public void failMode(FailMode failMode) {
		this.failMode = failMode;
	}
	
	public boolean isRenameTable() {
		return renameTable;
	}

	public void setRenameTable(boolean renameTable) {
		this.renameTable = renameTable;
	}

	public String getFinalTable() {
		return finalTable;
	}

	public void setFinalTable(String t) {
		this.finalTable = t;
	}

	public void init() {
		if(StringUtils.isBlank(sql) && StringUtils.isBlank(table)) {
			throw new IllegalStateException("table or sql must be not blank");
		}
		
		JdbcUtil.executeWithSemicolonComma(getDataSource(),beforeSql);
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
		
		JdbcTemplate jdbcTemplate = getJdbcTemplate();
		JdbcUtil.executeWithSemicolonComma(getDataSource(), sessionSql);
		
		Map allColumnsWithValue = getAllColumnWithValue(rows);
		
		Map<String,String> localColumnsSqlType = JdbcDataTypeUtil.getDatabaseDataType(cacheJdbcUrl(), allColumnsWithValue,this.columnsSqlType,this.defaultColumnSqlType);
		if(autoCreateTable) {
			executeCreateTableSql(jdbcTemplate,localColumnsSqlType);
		}
		
		Set<String> tableColumnNames = localColumnsSqlType.keySet();
		
		if(autoAlterTableAddColumn) {
			BiConsumer<String, String> alterTableAddColumnAction = newAlterTableAddColumnFunction(jdbcTemplate);
			JdbcUtil.alterTableIfColumnMiss(jdbcTemplate, allColumnsWithValue,table,cacheJdbcUrl(),this.columnsSqlType,this.defaultColumnSqlType,alterTableAddColumnAction);
		}
		
		return generateSql(jdbcTemplate, tableColumnNames);
	}

	protected BiConsumer<String, String> newAlterTableAddColumnFunction(JdbcTemplate jdbcTemplate) {
		return JdbcUtil.newAlterTableAddColumnFunction(jdbcTemplate, table);
	}

	protected Map getAllColumnWithValue(final List<Object> rows) {
		List tmpRows = new ArrayList(rows);
		Map allColumnsWithValue = MapUtil.mergeAllMapWithNotNullValue((List) tmpRows);
		
		MapUtil.putWithNotNullValue(this._allColumnWithValue, allColumnsWithValue);
		
		return allColumnsWithValue;
	}

	protected String generateSql(JdbcTemplate jdbcTemplate, Set<String> tableColumnNames) {
		if(OutputMode.delete == outputMode) {
			return JdbcSqlUtil.buildDeleteSql(table, getPrimaryKeysArray());
		}
		
		Collection<String> columns = getColumnsByColumnsFrom(jdbcTemplate,tableColumnNames);
		Assert.notEmpty(columns,"columns must be not empty");
		
		String resultSql = null;
		if(OutputMode.insert == outputMode) {
			resultSql = JdbcSqlUtil.buildInsertSql(table, columns);
		}else if(OutputMode.replace == outputMode) {
			resultSql = JdbcSqlUtil.buildMysqlInsertOrUpdateSql(table, columns, getPrimaryKeysArray());
		}else if(OutputMode.update == outputMode) {
			resultSql = JdbcSqlUtil.buildUpdateSql(table, columns, getPrimaryKeysArray());
		}else {
			throw new UnsupportedOperationException("error outputMode:"+outputMode);
		}
		
		return resultSql;
	}

	protected Collection<String> getColumnsByColumnsFrom(JdbcTemplate jdbcTemplate,Set<String> tableColumnNames) {
		if(ColumnsFrom.input == columnsFrom) {
			return tableColumnNames;
		}else if(ColumnsFrom.table == columnsFrom) {
			return JdbcUtil.getTableColumnsName(jdbcTemplate, table, cacheJdbcUrl());
		}else if(ColumnsFrom.config == columnsFrom) {
			Assert.hasText(columns,"columns must be not blank if: columnsFrom == config");
			
			String[] splitTableColumns = JdbcUtil.splitTableColumns(columns);
			if(splitTableColumns == null) return null;
			return Arrays.asList(splitTableColumns);
		}else {
			throw new UnsupportedOperationException("error ColumnsFrom:" + columnsFrom);
		}
	}

	protected void executeCreateTableSql(JdbcTemplate jdbcTemplate,Map<String,String> columnsSqlType) {
		if(_executedCreateTable) return;
		
		String createTableSql = buildCreateTableSql(columnsSqlType);
		
		try {
			if(StringUtils.isBlank(createTableSql)) {
				return;
			}
			
			if(!JdbcUtil.tableExists(jdbcTemplate,table,cacheJdbcUrl())) {
				jdbcTemplate.execute(createTableSql);
				logger.info("executeCreateTableSql() "+ createTableSql);
			}
		}catch(Exception e) {
			logger.warn("execute create table sql error:"+createTableSql,e);
		}finally {
			_executedCreateTable = true;
		}
		
	}

	protected String buildCreateTableSql(Map<String, String> columnsSqlType) {
		String createTableSql = JdbcCreateTableSqlUtil.buildCreateTableSql(table, columnsComment, columnsSqlType, getPrimaryKeysArray());
		return createTableSql;
	}
	
	private String[] _primaryKeyArray;
	protected String[] getPrimaryKeysArray() {
		if(_primaryKeyArray == null) {
			if(StringUtils.isBlank(primaryKeys)) {
				List<String> tablePrimaryKeysList = JdbcUtil.getTablePrimaryKeysList(table,getJdbcTemplate());
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
		if(rows.size() >= 1000 || costTime >= 1000) {
			long tps = Util.getTPS(rows.size(), costTime);
			logger.info("execute update sql with rows:"+rows.size()+" costTimeMills:"+costTime+" tps:"+ tps +" for sql:"+getOutputDigestInfo(finalSql));
		}
	}

	private String getOutputDigestInfo(String finalSql) {
		if(StringUtils.isNotBlank(table))
			return table;
		
		return StringUtils.substring(finalSql,0,100);
	}

	protected String executeWithJdbc(final List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return null;
		
		String finalSql = alterTableAndGetFinalSql(rows);
		
		List<List> multiChunkRows = CollectionUtil.chunk(rows, batchSize);
		
		try {
			failMode.forEach(multiChunkRows,executeWithJdbc(finalSql));
		}catch(Exception e) {
			exception = e;
			JdbcUtil.tableColumnsCache.clear(); //清下表字段缓存，避免字段出错
			throw new RuntimeException(e);
		}
		
		return finalSql;
	}

	private Consumer<List> executeWithJdbc(final String finalSql) {
		Assert.hasText(finalSql,"finalSql must be no blank");
		
		return (finalRows) -> {
			if(CollectionUtils.isEmpty(finalRows)) {
				return;
			}
			
			if(replaceSqlWithParams) {
				ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(finalSql);
				for(Object row : finalRows) {
					execWithReplacedSql(parsedSql, row);
				}
			}else {
				
				final String[] updateSqls = StringUtils.split(finalSql,JdbcUtil.SQL_SEPARATOR_CHAR);
				TransactionTemplate tt = getTransactionTemplate();
				
				tt.execute(new TransactionCallback<Object>() {
					public Object doInTransaction(TransactionStatus status) {
						JdbcUtil.executeWithSemicolonComma(getDataSource(),lockSql);
						
						SqlParameterSource[] batchArgs = newSqlParameterSource(finalRows);
						
						for(final String updateSql : updateSqls) {
							if(StringUtils.isBlank(updateSql)) 
								continue;
							
							try {
								getNamedParameterJdbcTemplate().batchUpdate(updateSql, batchArgs);
							}catch(Exception e) {
								throw new RuntimeException("execute sql error,sql:"+updateSql+" firstRow:"+finalRows.get(0),e);
							}
						}
						return true;
					}
	
	
				});
			}
		};
	}

	private void execWithReplacedSql(ParsedSql parsedSql, Object row) {
		String replacedSql = JdbcUtil.getReplacedSql(parsedSql, row);
		getJdbcTemplate().execute(replacedSql);
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
	
	@Override
	public void close() {
		
		boolean isSuccess = exception != null;
		
		if(isSuccess) {
			DataSource dataSource = getDataSource();
	
			executeRenameTableSqls(dataSource);
			
			JdbcUtil.executeWithSemicolonComma(dataSource,afterSql);
			logger.info("executed afterSql:"+afterSql);
		}
		
	}

	protected String getBackupFinalTable() {
		return "bak_by_output_" + finalTable;
	}
	
	protected void executeRenameTableSqls(DataSource dataSource) {
		if(!renameTable) {
			return;
		}
		
		Assert.hasText(finalTable,"renameTable = true, finalTable must be not blank");
		
		if(StringUtils.equals(table, finalTable)) {
			return;
		}
			
		String backupFinalTable = getBackupFinalTable();
		if(JdbcUtil.tableExists(getJdbcTemplate(), backupFinalTable, cacheJdbcUrl())) {
			String dropBakTableSql = "DROP TABLE " + backupFinalTable + ";\n";
			JdbcUtil.executeWithSemicolonComma(dataSource,dropBakTableSql);
			logger.info("executed sql for table rename:" + dropBakTableSql);
		}
		
		if(JdbcUtil.tableExists(getJdbcTemplate(), finalTable, cacheJdbcUrl())) {
			String renameSql = getTableRenameSql(finalTable,backupFinalTable,cacheJdbcUrl())+";\n";
			JdbcUtil.executeWithSemicolonComma(dataSource,renameSql);
			logger.info("executed sql for table rename:" + renameSql);
		}
		
		String renameSql = getTableRenameSql(table,finalTable,cacheJdbcUrl())+";\n";
		JdbcUtil.executeWithSemicolonComma(dataSource,renameSql);
		logger.info("executed sql for table rename:" + renameSql);
	}
	
	protected String getTableRenameSql(String oldTableName,String newTableName,String jdbcUrl) {
		return JdbcUtil.getTableRenameSql(oldTableName, newTableName, jdbcUrl);
	}

	
}
