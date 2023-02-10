package com.github.dataswitch.util.model;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.util.StringHelper;
/**
 * 
 * @author badqiu
 * @email badqiu(a)gmail.com
 */
public class Table {

	String sqlName;
	String className;
	/** the name of the owner of the synonym if this table is a synonym */
	private String ownerSynonymName = null;
	
	private String remarks;
	
	List<Column> columns = new ArrayList();
	List<Column> primaryKeyColumns = new ArrayList();

	public void setClassName(String className) {
		this.className = className;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public String getOwnerSynonymName() {
		return ownerSynonymName;
	}
	public void setOwnerSynonymName(String ownerSynonymName) {
		this.ownerSynonymName = ownerSynonymName;
	}
	public List getPrimaryKeyColumns() {
		return primaryKeyColumns;
	}
	public void setPrimaryKeyColumns(List primaryKeyColumns) {
		this.primaryKeyColumns = primaryKeyColumns;
	}
	public String getSqlName() {
		return sqlName;
	}
	public void setSqlName(String sqlName) {
		this.sqlName = sqlName;
	}
	
	public String getRemarks() {
		return remarks;
	}
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	public void addColumn(Column column) {
		columns.add(column);
	}
	
	public boolean isSingleId() {
		int pkCount = 0;
		for(int i = 0; i < columns.size(); i++) {
			Column c = (Column)columns.get(i);
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
			Column c = (Column)columns.get(i);
			if(c.isPk())
				results.add(c);
		}
		return results;
	}
	
	public Column getIdColumn() {
		List columns = getColumns();
		for(int i = 0; i < columns.size(); i++) {
			Column c = (Column)columns.get(i);
			if(c.isPk())
				return c;
		}
		return null;
	}
	
	
	public List<Column> getUniqueColumns() {
		return columns.stream().filter(c->c.isUnique()).collect(Collectors.toList());
	}
	
	
	public    static final String PKTABLE_NAME  = "PKTABLE_NAME";
	public    static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
	public    static final String FKTABLE_NAME  = "FKTABLE_NAME";
	public    static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
	public    static final String KEY_SEQ       = "KEY_SEQ";
}
