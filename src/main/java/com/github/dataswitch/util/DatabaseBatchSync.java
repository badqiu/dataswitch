package com.github.dataswitch.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.sql.DataSource;

import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.input.JdbcInput;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.output.Output;

public class DatabaseBatchSync implements Function<Map<String,Object>, Void>{

	private DataSource inputDataSource;
	
	private DataSource outputDataSource;
	
	private String includeTables;
	private String excludeTables;
	
	private Class<Output> outputClass;
	private FailMode failMode = FailMode.FAIL_FAST;
	
	@Override
	public Void apply(Map<String, Object> t) {
		try {
			return process(t);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	private Void process(Map<String, Object> params) throws Exception {
		
		List<String> tables = getAllTableNames(inputDataSource);
		tables = filterByIncludeExclude(tables,includeTables,excludeTables);
		
		List<InputsOutputs> inputsOutputsList = buildInputsOutputs(tables);
		
		failMode.forEach(inputsOutputsList, (item) -> {
			item.exec(params);
		});
		
		return null;
	}

	protected List<InputsOutputs> buildInputsOutputs(List<String> tables)
			throws InstantiationException, IllegalAccessException {
		List<InputsOutputs> inputsOutputsList = new ArrayList();
		
		for(String tableName : tables) {
			JdbcInput jdbcInput = new JdbcInput();
			jdbcInput.setTable(tableName);
			jdbcInput.setDataSource(inputDataSource);
			
			Output output = buildOutput(jdbcInput,tableName);
			
			InputsOutputs inputsOutputs = new InputsOutputs();
			inputsOutputs.setInput(jdbcInput);
			inputsOutputs.setOutput(output);
			
			inputsOutputsList.add(inputsOutputs);
		}
		return inputsOutputsList;
	}

	protected Output buildOutput(JdbcInput input,String tableName) {
		JdbcOutput jdbcOutput = null;
		jdbcOutput.setTable(tableName);
//			jdbcOutput.setColumnsFrom(columnsFrom);
		jdbcOutput.failMode(FailMode.FAIL_FAST);
		configOutput(jdbcOutput);
		return jdbcOutput;
	}

	protected void configOutput(Output jdbcOutput) {
		
	}

	protected List<String> filterByIncludeExclude(List<String> tables, String includeTables2, String excludeTables2) {
		return tables;
	}

	public static List<String> getAllTableNames(DataSource dataSource) throws SQLException {
		List<Map<String, Object>> tables = getAllTables(dataSource);
		
		List<String> tableNames = new ArrayList<String>();
		for(Map table : tables) {
			String tableName = (String)table.get("TABLE_NAME");
			tableNames.add(tableName);
		}
		return tableNames;
	}
	
	public static List<Map<String, Object>> getAllTables(DataSource dataSource) throws SQLException {
		Connection connection = dataSource.getConnection();
		DatabaseMetaData metaData = connection.getMetaData();
		metaData.getSchemas();
		metaData.getCatalogs();
		
		ResultSet rs = metaData.getTables(null, null, null, null);
		List<Map<String,Object>> tables = JdbcUtil.resultSet2List(rs);
		return tables;
	}
	
	
}
