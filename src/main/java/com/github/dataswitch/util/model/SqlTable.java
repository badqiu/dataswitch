package com.github.dataswitch.util.model;


import java.util.ArrayList;
import java.util.List;
/**
 * 
 * @author badqiu
 * @email badqiu(a)gmail.com
 */
public class SqlTable {
	private String catalog = null;
	private String schema = null;
	private String sqlName;
	private String className;
	private List<SqlColumn> columns = new ArrayList<SqlColumn>();
	private List<SqlColumn> primaryKeyColumns = new ArrayList<SqlColumn>();
	
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public List<SqlColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<SqlColumn> columns) {
		this.columns = columns;
	}

	public List<SqlColumn> getPrimaryKeyColumns() {
		return primaryKeyColumns;
	}
	public void setPrimaryKeyColumns(List<SqlColumn> primaryKeyColumns) {
		this.primaryKeyColumns = primaryKeyColumns;
	}
	public String getSqlName() {
		return sqlName;
	}
	public void setSqlName(String sqlName) {
		this.sqlName = sqlName;
	}
	
	public void addColumn(SqlColumn column) {
		columns.add(column);
	}
	
	public boolean isSingleId() {
		int pkCount = 0;
		for(int i = 0; i < columns.size(); i++) {
			SqlColumn c = (SqlColumn)columns.get(i);
			if(c.isPk()) {
				pkCount ++;
			}
		}
		return pkCount > 1 ? false : true;
	}
	
	public boolean isCompositeId() {
		return !isSingleId();
	}
	
	public List getCompositeIdColumns() {
		List results = new ArrayList();
		List columns = getColumns();
		for(int i = 0; i < columns.size(); i++) {
			SqlColumn c = (SqlColumn)columns.get(i);
			if(c.isPk())
				results.add(c);
		}
		return results;
	}
	
	public SqlColumn getIdColumn() {
		List columns = getColumns();
		for(int i = 0; i < columns.size(); i++) {
			SqlColumn c = (SqlColumn)columns.get(i);
			if(c.isPk())
				return c;
		}
		return null;
	}
	
}
