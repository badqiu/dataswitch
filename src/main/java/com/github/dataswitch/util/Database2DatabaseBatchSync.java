package com.github.dataswitch.util;

import java.lang.reflect.InvocationTargetException;

import com.github.dataswitch.util.BeanUtils;

import com.github.dataswitch.input.JdbcInput;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.support.DataSourceProvider;

/**
 *  数据库与数据库的批量同步
 * 
 * @author badqiu
 *
 */
public class Database2DatabaseBatchSync extends DatabaseBatchSync {
	
	private DataSourceProvider outputDataSource = new DataSourceProvider();
	
	private JdbcOutput outputTemplate = new JdbcOutput();
	
	private boolean renameTable = true;
	
	private String outputTablePrefix = "";
	private String outputTableSuffix = "";
	
	
	public DataSourceProvider getOutputDataSource() {
		return outputDataSource;
	}

	public void setOutputDataSource(DataSourceProvider outputDataSource) {
		this.outputDataSource = outputDataSource;
	}

	public JdbcOutput getOutputTemplate() {
		return outputTemplate;
	}

	public void setOutputTemplate(JdbcOutput outputTemplate) {
		this.outputTemplate = outputTemplate;
	}

	@Override
	protected Output buildOutput(JdbcInput input,String tableName) throws Exception {
		JdbcOutput jdbcOutput = new JdbcOutput();
		BeanUtils.copyProperties(jdbcOutput, outputTemplate);
		
		configRenameTable(tableName, jdbcOutput);
		jdbcOutput.setDataSource(outputDataSource.getDataSource());

		//jdbcOutput.setColumnsFrom(columnsFrom);
		//jdbcOutput.failMode(FailMode.FAIL_FAST);
		configOutput(tableName,jdbcOutput);
		return jdbcOutput;
	}

	protected void configRenameTable(String tableName, JdbcOutput jdbcOutput) {
		jdbcOutput.setRenameTable(renameTable);
		
		tableName = getRealTableName(tableName);
		if(jdbcOutput.isRenameTable()) {
			jdbcOutput.setTable("tmp_by_output_" + tableName);
			jdbcOutput.setFinalTable(tableName);
		}else {
			jdbcOutput.setTable(tableName);
			jdbcOutput.setFinalTable(tableName);
		}
	}
	
	public String getRealTableName(String tableName) {
		return outputTablePrefix + tableName + outputTableSuffix;
	}
	
}
