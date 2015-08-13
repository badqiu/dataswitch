package com.github.dataswitch.processor;

import java.util.List;

import com.github.dataswitch.util.ObjectSqlQueryUtil;

public class SqlProcessor implements Processor {

	private String sql;

	public SqlProcessor(){
	}
	
	public SqlProcessor(String sql) {
		super();
		this.sql = sql;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		List result = ObjectSqlQueryUtil.query(sql, (List) datas);
		return result;
	}

}
