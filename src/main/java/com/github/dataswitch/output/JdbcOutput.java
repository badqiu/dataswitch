package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.github.dataswitch.util.ParsedSql;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.dataswitch.util.DataSourceProvider;
import com.github.dataswitch.util.NamedParameterUtils;
import com.github.rapid.common.beanutils.PropertyUtils;

public class JdbcOutput extends DataSourceProvider implements Output {

	private static Logger logger = LoggerFactory.getLogger(JdbcOutput.class);
	private String lockSql;
	private String sql;
	private String beforeSql;
	private String afterSql;
	
	/**
	 * 是否将命名参数替换成实际值
	 */
	private boolean replaceSqlWithParams = false;
	
	private transient boolean isInit = false;
	private TransactionTemplate transactionTemplate;
	
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

	public void init() {
		executeWithSemicolonComma(getDataSource(),beforeSql);
		logger.info("executed beforeSql:"+beforeSql);
	}
	
	@Override
	public void write(final List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		if(!isInit) {
			synchronized(this) {
				isInit = true;
				init();
			}
		}
		
		executeWithJdbc(rows);
	}

	protected void executeWithJdbc(final List<Object> rows) {
		if(replaceSqlWithParams) {
			ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
			for(Object row : rows) {
				execWithReplacedSql(parsedSql, row);
			}
		}else {
			final String[] sqlArray = StringUtils.split(this.sql,";");
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
	}

	private void execWithReplacedSql(ParsedSql parsedSql, Object row) {
		String replacedSql = getReplacedSql(parsedSql, row);
//		new NamedParameterJdbcTemplate(getDataSource()).execute(replacedSql, paramMap, action)
		new JdbcTemplate(getDataSource()).execute(replacedSql);
	}

	public static String getReplacedSql(ParsedSql parsedSql, Object row) {
		String replacedSql = parsedSql.getOriginalSql();
		for(String name : parsedSql.getParameterNames()) {
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

	private static SqlParameterSource[] newSqlParameterSource(final List<Object> rows) {
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
	
	private static void executeWithSemicolonComma(DataSource ds, String sql) {
		if (StringUtils.isNotBlank(sql)) {
			String[] sqls = sql.split(";");
			for (String s : sqls) {
				if(StringUtils.isNotBlank(s)) {
					new JdbcTemplate(ds).execute(s);
				}
			}
		}
	}
	
	@Override
	public void close() {
		executeWithSemicolonComma(getDataSource(),afterSql);
		logger.info(" executed afterSql:"+afterSql);
	}
	
}
