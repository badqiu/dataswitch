package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.github.dataswitch.support.DataSourceProvider;

/**
 * 同步表的列字段
 * 
 * @author badqiu
 *
 */
public class DatabaseTableColumnsSync {
	
	private DataSourceProvider inputDataSource = new DataSourceProvider();
	private DataSourceProvider outputDataSource = new DataSourceProvider();
	private String defaultSqlType;
	
	public void syncTableColumns(String inputTable,String outputTable) {
		Map<String,String> inputColumns = JdbcUtil.getTableColumns(new JdbcTemplate(inputDataSource.getDataSource()), inputTable, inputDataSource.cacheJdbcUrl());
		JdbcTemplate targetJdbcTemplate = new JdbcTemplate(outputDataSource.getDataSource());
		
//		Map<String,String> outputColumns = JdbcUtil.getTableColumns(targetJdbcTemplate, outputTable, outputDataSource.cacheJdbcUrl());
		JdbcUtil.alterTableIfColumnMiss(targetJdbcTemplate, newColumnsWithData(inputColumns), outputTable, outputDataSource.cacheJdbcUrl(), null,defaultSqlType);
	}

	private Map newColumnsWithData(Map<String, String> inputColumns) {
		Map map = new HashMap();
		return null;
	}
}
