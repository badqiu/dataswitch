package com.github.dataswitch.output;

import com.github.dataswitch.serializer.TxtSerializer;

public class HdfsTxtOutput extends HdfsOutput{

	private TxtSerializer txtSerializer = new TxtSerializer();

	public HdfsTxtOutput() {
		setSerializer(txtSerializer);
	}

	public String getNullValue() {
		return txtSerializer.getNullValue();
	}

	public void setNullValue(String nullValue) {
		txtSerializer.setNullValue(nullValue);
	}

	public String getColumns() {
		return txtSerializer.getColumns();
	}

	public void setColumns(String columns) {
		txtSerializer.setColumns(columns);
	}

	public String getColumnSplit() {
		return txtSerializer.getColumnSplit();
	}

	public void setColumnSplit(String columnSplit) {
		txtSerializer.setColumnSplit(columnSplit);
	}

	public String getLineSplit() {
		return txtSerializer.getLineSplit();
	}

	public void setLineSplit(String lineSplit) {
		txtSerializer.setLineSplit(lineSplit);
	}

	public String getCharset() {
		return txtSerializer.getCharset();
	}

	public void setCharset(String charset) {
		txtSerializer.setCharset(charset);
	}
	
}
