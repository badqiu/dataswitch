package com.github.dataswitch.util;

import com.github.dataswitch.input.JdbcInput;
import com.github.dataswitch.output.JdbcOutput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.support.DataSourceProvider;
import com.github.rapid.common.beanutils.BeanUtils;

public class Database2DatabaseBatchSync extends DatabaseBatchSync {
	
	private DataSourceProvider outputDataSource = new DataSourceProvider();
	
	private JdbcOutput outputTemplate = new JdbcOutput();
	
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
	protected Output buildOutput(JdbcInput input,String tableName) {
		JdbcOutput jdbcOutput = new JdbcOutput();
		BeanUtils.copyProperties(jdbcOutput, outputTemplate);
		
		jdbcOutput.setTable(tableName);
		jdbcOutput.setDataSource(outputDataSource.getDataSource());

		//jdbcOutput.setColumnsFrom(columnsFrom);
		//jdbcOutput.failMode(FailMode.FAIL_FAST);
		configOutput(jdbcOutput);
		return jdbcOutput;
	}
	
	
	
}
