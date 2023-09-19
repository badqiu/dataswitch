package com.github.dataswitch.output;

import com.github.dataswitch.serializer.TxtSerializer;

public class TxtFileOutput extends FileOutput {

	private TxtSerializer txtSerializer = new TxtSerializer();

	public TxtFileOutput() {
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

	public String getColumnSeparator() {
		return txtSerializer.getColumnSeparator();
	}

	public void setColumnSeparator(String columnSeparator) {
		txtSerializer.setColumnSeparator(columnSeparator);
	}

	public String getLineSplit() {
		return txtSerializer.getLineSeparator();
	}

	public void setLineSplit(String lineSplit) {
		txtSerializer.setLineSeparator(lineSplit);
	}

	public String getCharset() {
		return txtSerializer.getCharset();
	}

	public void setCharset(String charset) {
		txtSerializer.setCharset(charset);
	}

}
