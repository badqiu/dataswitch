package com.github.dataswitch.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.AntPathMatcher;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.InputsOutputs;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.input.JdbcInput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.support.DataSourceProvider;

/**
 * 数据库的批量表数据同步
 * 
 * @author badqiu
 *
 */
public class DatabaseBatchSync extends BaseObject implements Function<Map<String,Object>, KeyValue<List<String>,List<String>>>{

	private DataSourceProvider inputDataSource = new DataSourceProvider();
	
	private String includeTables = "*";
	private String excludeTables;
	
	private Class<Output> outputClass; //输出类
	private String outputProps; //输出配置属性
	private FailMode failMode = FailMode.FAIL_AT_END;
	
	private String configScript;
	private String configLangguage;
	
	private JdbcInput inputTemplate = new JdbcInput();
	
	@Override
	public KeyValue<List<String>,List<String>> apply(Map<String, Object> t) {
		try {
			KeyValue<List<String>,List<String>> result = process(t);
			return result;
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	private KeyValue<List<String>,List<String>> process(Map<String, Object> params) throws Exception {
		
		List<String> tables = getAllTableNames(inputDataSource.getDataSource());
		tables = filterByIncludeExclude(tables,includeTables,excludeTables);
		
		List<InputsOutputs> inputsOutputsList = buildInputsOutputsList(tables);
		
		List<String> successList = new ArrayList();
		List<String> errorList = new ArrayList();
		failMode.forEach(inputsOutputsList, (item) -> {
			
			String table = getTableFromInput0(item);
			
			try {
				item.exec(params);
				successList.add(table);
			}catch(Exception e) {
				errorList.add(table);
				throw e;
			}
		});
		
		return new KeyValue(successList,errorList);
	}

	private String getTableFromInput0(InputsOutputs item) {
		JdbcInput jdbcInput = (JdbcInput)item.getInputs()[0];
		String table = jdbcInput.getTable();
		return table;
	}

	protected List<InputsOutputs> buildInputsOutputsList(List<String> tables)
			throws Exception {
		if(CollectionUtils.isEmpty(tables)) {
			return Collections.EMPTY_LIST;
		}
		
		List<InputsOutputs> inputsOutputsList = new ArrayList();
		
		for(String tableName : tables) {
			JdbcInput jdbcInput = buildJdbcInput(tableName);
			Output output = buildOutput(jdbcInput,tableName);
			InputsOutputs inputsOutputs = buildInputsOutputs(jdbcInput, output);
			
			inputsOutputsList.add(inputsOutputs);
		}
		
		return inputsOutputsList;
	}

	private InputsOutputs buildInputsOutputs(JdbcInput jdbcInput, Output output) {
		InputsOutputs inputsOutputs = new InputsOutputs();
		inputsOutputs.setInput(jdbcInput);
		inputsOutputs.setOutput(output);
		return inputsOutputs;
	}

	private JdbcInput buildJdbcInput(String tableName) throws Exception {
		JdbcInput jdbcInput = new JdbcInput();
		BeanUtils.copyProperties(jdbcInput, inputTemplate);
		
		jdbcInput.setTable(tableName);
		jdbcInput.setDataSource(inputDataSource.getDataSource());
		return jdbcInput;
	}

	protected Output buildOutput(JdbcInput input,String tableName) throws Exception {
		try {
			Output output = outputClass.newInstance();
			configOutput(tableName,output);
			return output;
		} catch (Exception e) {
			throw new RuntimeException("buildOutput error on tableName:"+tableName,e);
		} 
	}

	protected void configOutput(String tableName,Output output) {
		Properties props = PropertiesUtil.createProperties(outputProps);
		props.put("table", tableName);
		BeanUtils.copyProperties(output, props);
		
		Map map = new HashMap();
		map.put("output", output);
		map.put("table", tableName);
		
		ScriptEngineUtil.eval(configLangguage, configScript,map);
		
	}

	protected static List<String> filterByIncludeExclude(List<String> tables, String includeTables, String excludeTables) {
		List<String> result = new ArrayList<String>();
		for(String table : tables) {
			if (isMatchMany(excludeTables,table)) {
				continue;
			}
			if (isMatchMany(includeTables,table)) {
				result.add(table);
			}
		}
		return result;
	}

	private static boolean isMatchMany(String matchPatterns, String table) {
		String[] array = Util.splitColumns(matchPatterns);
		for(String pattern : array) {
			if(isMatch(pattern,table)) {
				return true;
			}
		}
		return false;
	}

	private static AntPathMatcher antPathMatcher = new AntPathMatcher(".");
	static {
		antPathMatcher.setTrimTokens(true);
		antPathMatcher.setCaseSensitive(false);
	}
	
	private static boolean isMatch(String matchPattern, String table) {
		if(StringUtils.isBlank(table)) return false;
		
		if(table.equals(matchPattern)) {
			return true;
		}
		
		return antPathMatcher.match(matchPattern, table);
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
