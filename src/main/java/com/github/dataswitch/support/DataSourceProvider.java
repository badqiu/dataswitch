package com.github.dataswitch.support;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.JdbcUtil;

public class DataSourceProvider extends BaseObject{

	private DataSource dataSource;
	private String username;
	private String password;
	private String url;
	private String driverClass;
	
	private transient TransactionTemplate transactionTemplate;
	
	public DataSource getDataSource() {
		if (dataSource == null) {
			this.dataSource = getDataSource(username,password,url,driverClass);
		}
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	
	public JdbcTemplate getJdbcTemplate() {
		return new JdbcTemplate(getDataSource());
	}

	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return new NamedParameterJdbcTemplate(getDataSource());
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
	
	private String _cacheJdbcUrl = null;
	public String cacheJdbcUrl()  {
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			_cacheJdbcUrl = getUrl();
		}
		if(StringUtils.isBlank(_cacheJdbcUrl)) {
			_cacheJdbcUrl = JdbcUtil.getJdbcUrl(getDataSource());
		}
		return _cacheJdbcUrl;
	}
	
	private static Map<String,DataSource> dataSourceCache = new HashMap<String,DataSource>();
	private static synchronized DataSource getDataSource(String username, String password, String url,String driverClass) {
		Assert.hasText(url,"jdbc url must be not empty");
		
		String dataSourceKey = url+username+password;
		DataSource result = dataSourceCache.get(dataSourceKey);
		if(result == null) {
			DriverManagerDataSource ds = new DriverManagerDataSource();
			ds.setDriverClassName(driverClass);
			ds.setUsername(username);
			ds.setPassword(password);
			ds.setUrl(url);
			dataSourceCache.put(dataSourceKey, ds);
			result = ds;
		}
		return result;
	}

}
