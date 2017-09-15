package com.github.dataswitch.input;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.Assert;

import com.github.dataswitch.util.DataSourceProvider;
import com.github.rapid.common.util.MapUtil;


public class JdbcInput extends DataSourceProvider implements Input{

	private static Logger logger = LoggerFactory.getLogger(JdbcInput.class);
	
	private String id;
	private String sql;
	private String table;
	private int fetchSize = 10000;
	private boolean mapKey2lowerCase = true;
	
	private transient ResultSet rs;
	private transient Connection conn;
	private transient ColumnMapRowMapper rowMapper = new ColumnMapRowMapper();
	private transient boolean isInit = false;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

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
	
	public boolean isMapKey2lowerCase() {
		return mapKey2lowerCase;
	}

	public void setMapKey2lowerCase(boolean mapKey2lowerCase) {
		this.mapKey2lowerCase = mapKey2lowerCase;
	}

	public void init() {
		rowMapper = new ColumnMapRowMapper();
		if(StringUtils.isBlank(sql)) {
			Assert.hasText(table,"table or sql must be not empty");
			sql = "select * from " + table;
		}
		
		Assert.hasText(sql,"sql must be not empty");
		Assert.notNull(getDataSource(),"dataSource must be not null");
		try {
			conn = getDataSource().getConnection();
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setFetchSize(fetchSize);
			
			long start = System.currentTimeMillis();
			rs = ps.executeQuery();
			long cost = System.currentTimeMillis() - start;
			
			logger.info("execute sql:"+sql+" cost time mills:"+cost);
		}catch(Exception e) {
			throw new RuntimeException("executeQuery error,sql:"+sql,e);
		}
	}
	
	@Override
	public List<Object> read(int size) { // TODO 可以继承BaseInput,删除该方法
		List result = new ArrayList<Map>();
		for(int i = 0; i < size; i++) {
			Map map = read();
			if(map == null) {
				break;
			}
			result.add(map);
		}
		
		if(mapKey2lowerCase) {
			return (List)MapUtil.allMapKey2LowerCase(result);
		}else {
			return (List)result;
		}
	}
	
	public Map read() {
		if(!isInit) {
			isInit = true;
			init();
		}
		try {
			if(rs.next()) {
				return rowMapper.mapRow(rs,0);
			}
			return null;
		}catch(Exception e) {
			throw new RuntimeException("read error,sql:"+sql,e);
		}
	}
	
	@Override
	public void close() {
		JdbcUtils.closeConnection(conn);
		JdbcUtils.closeResultSet(rs);
	}
	
}
