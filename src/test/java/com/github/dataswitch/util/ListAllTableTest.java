package com.github.dataswitch.util;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
	public void test_all_table_add_column_sql() throws Exception {
	
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
			
//			System.out.println(alterTableAddColumnSql);
			
			System.out.println(tableName+","+table.getColumns().size()+","+table.getUniqueColumns().size()+","+table.getRemarks());
		});;
		
		
	}
	
	@Test
	public void test_all_table_unique_keys_sql() throws Exception {
	
		DbModelProvider dbModelProvider = new DbModelProvider(inputDataSource.getDataSource());
		dbModelProvider.getAllTables().forEach(table -> {
			String tableName = table.getSqlName();
			Map<String,String> inputColumns = JdbcUtil.getTableColumns(new JdbcTemplate(inputDataSource.getDataSource()), tableName, inputDataSource.cacheJdbcUrl());
//			if(inputColumns.containsKey("company_id")) {
//				return;
//			}
			
//			String alterTableAddColumnSql = "ALTER TABLE " + tableName + " " + 
//					"ADD COLUMN `company_id` bigint NOT NULL DEFAULT 1 COMMENT '公司ID' ," + 
//					"ADD INDEX `idx_company_id`(`company_id`);";
			
//			System.out.println(alterTableAddColumnSql);
			
//			System.out.println(tableName+","+table.getColumns().size()+","+table.getUniqueColumns().size()+","+table.getRemarks());
			if(table.getUniqueIndexMaxColumns() <= 1) {
				return;
			}
			
			table.getUniqueIndexs().forEach((index,columns) -> {
				if(columns.contains("company_id")) {
					return;
				}
				if(columns.size() <= 1) {
					return;
				}
				if("PRIMARY".equals(index)) {
					return;
				}
				
				
				String sql = "ALTER TABLE "+table.getSqlName()+" " + 
						"DROP INDEX "+index+"," + 
						"ADD UNIQUE INDEX "+index + "(company_id, " + StringUtils.join(columns,",") + ") USING BTREE; -- "+table.getRemarks();
				
				System.out.println(sql);
				System.out.println();
			});
			
//			System.out.println(tableName+","+table.getUniqueIndexMaxColumns()+","+table.getRemarks());
		});;
		
		
	}
}
