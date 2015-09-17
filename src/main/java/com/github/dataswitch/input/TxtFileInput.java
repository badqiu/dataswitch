package com.github.dataswitch.input;

import com.github.dataswitch.serializer.TxtDeserializer;

public class TxtFileInput extends FileInput {

	public TxtFileInput() {
		setDeserializer(new TxtDeserializer());
	}
	
	public TxtDeserializer getTxtDeserializer() {
		return (TxtDeserializer)getDeserializer();
	}

	public String getColumnSplit() {
		return getTxtDeserializer().getColumnSplit();
	}

	public void setColumnSplit(String columnSplit) {
		getTxtDeserializer().setColumnSplit(columnSplit);
	}

	public String getNullValue() {
		return getTxtDeserializer().getNullValue();
	}

	public void setNullValue(String nullValue) {
		getTxtDeserializer().setNullValue(nullValue);
	}

	public String getColumns() {
		return getTxtDeserializer().getColumns();
	}

	public void setColumns(String columns) {
		getTxtDeserializer().setColumns(columns);
	}

	public String getCharset() {
		return getTxtDeserializer().getCharset();
	}

	public void setCharset(String charset) {
		getTxtDeserializer().setCharset(charset);
	}

	public int getSkipLines() {
		return getTxtDeserializer().getSkipLines();
	}

	public void setSkipLines(int skipLines) {
		getTxtDeserializer().setSkipLines(skipLines);
	}

}
