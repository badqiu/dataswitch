package com.github.dataswitch.util;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.dataswitch.support.DataSourceProvider;

public class ListAllTableTest {
	
	private DataSourceProvider inputDataSource = new DataSourceProvider();
	@Before 
	public void before() {
		inputDataSource.setUsername("fmuser");
		inputDataSource.setPassword("fmpass");
		inputDataSource.setUrl("jdbc:mysql://172.17.38.109:3306/vod_ad?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
		inputDataSource.setDriverClass("com.mysql.jdbc.Driver");
	}
	
	@Test
	public void test() throws Exception {
	
		DbModelProvider dbModelProvider = new DbModelProvider(inputDataSource.getDataSource());
		dbModelProvider.getAllTables().forEach(table -> {
			String tableName = table.getSqlName();
			Map<String,String> inputColumns = JdbcUtil.getTableColumns(new JdbcTemplate(inputDataSource.getDataSource()), tableName, inputDataSource.cacheJdbcUrl());
			if(inputColumns.containsKey("company_id")) {
				return;
			}
			String alterTableAddColumnSql = "ALTER TABLE " + tableName + " " + 
					"ADD COLUMN `company_id` bigint NOT NULL DEFAULT 1 COMMENT '公司ID' ," + 
					"ADD INDEX `idx_company_id`(`company_id`);";
			
			System.out.println(alterTableAddColumnSql);
		});;
		
		
	}
}
