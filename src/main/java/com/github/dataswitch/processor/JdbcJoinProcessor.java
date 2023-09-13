package com.github.dataswitch.processor;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.dataswitch.support.DataSourceProvider;

public class JdbcJoinProcessor extends JoinProcessor {

	private DataSourceProvider dataSource = new DataSourceProvider();
	
	private String sql; //查询SQL，优先级高于table
	private String table; //查询table，优先级低于sql
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public DataSource getDataSource() {
		return dataSource.getDataSource();
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource.setDataSource(dataSource);
	}

	public String getUsername() {
		return dataSource.getUsername();
	}

	public void setUsername(String username) {
		dataSource.setUsername(username);
	}

	public String getPassword() {
		return dataSource.getPassword();
	}

	public void setPassword(String password) {
		dataSource.setPassword(password);
	}

	public String getUrl() {
		return dataSource.getUrl();
	}

	public void setUrl(String url) {
		dataSource.setUrl(url);
	}

	public void setAuthor(String author) {
		dataSource.setAuthor(author);
	}

	public String getDriverClass() {
		return dataSource.getDriverClass();
	}

	public void setDriverClass(String driverClass) {
		dataSource.setDriverClass(driverClass);
	}

	public JdbcTemplate getJdbcTemplate() {
		return dataSource.getJdbcTemplate();
	}

	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return dataSource.getNamedParameterJdbcTemplate();
	}

	public TransactionTemplate getTransactionTemplate() {
		return dataSource.getTransactionTemplate();
	}

	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		initJoinDatas();
		super.open(params);
	}

	private void initJoinDatas() {
		List<Map> datas = (List)getJdbcTemplate().queryForList(getQuerySql());
		setJoinDatas(datas);
	}

	private String getQuerySql() {
		if(StringUtils.isNotBlank(sql)) {
			return sql;
		}
		if(StringUtils.isNotBlank(table)) {
			return "select * from "+table;
		}
		throw new RuntimeException("table or must must be not blank");
	}
	
}
