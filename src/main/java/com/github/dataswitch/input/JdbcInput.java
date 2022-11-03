package com.github.dataswitch.input;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.Assert;

import com.github.dataswitch.support.DataSourceProvider;
import com.github.dataswitch.util.JdbcUtil;
import com.github.dataswitch.util.MapUtil;


public class JdbcInput extends DataSourceProvider implements Input{

	private static Logger logger = LoggerFactory.getLogger(JdbcInput.class);
	
	private String id;
	private String sql;
	private String table;
	private int fetchSize = 1000;
	private boolean mapKey2lowerCase = true;
	
	private boolean addTableNameColumn = false;
	
	protected transient ResultSet _rs;
	protected transient Connection _conn;
	
	private transient ColumnMapRowMapper _rowMapper = new ColumnMapRowMapper() {
		protected String getColumnKey(String columnName) {
			return JdbcUtil.getFinalColumnKey(columnName);
		}
	};
	
	private int _rowNumber = 0;
	
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
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Input.super.open(params);
		init();
	}

	public void init() {
		_rowMapper = new ColumnMapRowMapper();
		if(StringUtils.isBlank(sql)) {
			Assert.hasText(table,"table or sql must be not empty");
			sql = "select * from " + table;
		}
		
		Assert.hasText(sql,"sql must be not empty");
		Assert.notNull(getDataSource(),"dataSource must be not null");
		try {
			_conn = getDataSource().getConnection();
			PreparedStatement ps = _conn.prepareStatement(sql);
			ps.setFetchSize(fetchSize);
			
			long start = System.currentTimeMillis();
			_rs = ps.executeQuery();
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
			Map row = read();
			if(row == null) {
				break;
			}
			result.add(row);
		}
		
		if(mapKey2lowerCase) {
			List collect = (List)result.stream().map(item -> {return MapUtil.keyToLowerCase((Map)item);}).collect(Collectors.toList());
			return collect;
//			return (List)MapUtil.allMapKey2LowerCase(result);
		}else {
			return (List)result;
		}
	}
	
	public Map read() {
		try {
			if(_rs == null) return null;
			
			if(_rs.next()) {
				Map<String, Object> mapRow = _rowMapper.mapRow(_rs,_rowNumber++);
				mapRow = processRow(mapRow);
				return mapRow;
			}
			return null;
		}catch(Exception e) {
			throw new RuntimeException("read error,sql:"+sql,e);
		}
	}
	
	protected Map<String, Object> processRow(Map<String, Object> row) {
		processAddMetaData(row);
		
		return row;
	}

	protected void processAddMetaData(Map<String, Object> row) {
		if(row == null) return;
		
		if(addTableNameColumn) {
			String table = getFromTableName();
			row.put("fromTable", table);
		}
	}
	
	private String _metaTableName = null;
	private String getFromTableName()  {
		if(_metaTableName == null) {
			try {
				ResultSetMetaData metaData = _rs.getMetaData();
				_metaTableName = metaData.getTableName(1);
			}catch(Exception e) {
				logger.warn("ignore get table name from metadata error",e);
				_metaTableName = table;
			}
		}
		return _metaTableName;
	}

	@Override
	public void close() {
		JdbcUtils.closeResultSet(_rs);
		JdbcUtils.closeConnection(_conn);
	}
	
}
