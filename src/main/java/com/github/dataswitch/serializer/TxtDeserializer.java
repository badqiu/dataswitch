package com.github.dataswitch.serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.serializer.Deserializer;
import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.MapUtil;
import com.github.dataswitch.util.Util;

public class TxtDeserializer extends BaseObject implements Deserializer<Map>{

	/**
	 * 分隔符
	 **/
	private String columnSeparator = Constants.COLUMN_SPLIT;
	
	/**
	 * hive 的null 值特殊转义字符
	 */
	private String nullValue = Constants.NULL_VALUE;
	/**
	 * 数据列
	 */
	private String columns;
	
	/**
	 * 忽略跳过的列数
	 */
	private int skipLines = 0;
	
	private String charset = null;
	
	private transient String[] columnNames;
	private transient boolean isInit = false;

	private Map<InputStream,BufferedReader> cache = new HashMap<InputStream,BufferedReader>();

	
	public TxtDeserializer() {
	}
	
	public TxtDeserializer(TxtDeserializer in) {
		this.setColumns(in.getColumns());
		this.setNullValue(in.getNullValue());
		this.setColumnSeparator(in.getColumnSeparator());
		this.setCharset(in.getCharset());
		this.setSkipLines(in.getSkipLines());
	}
	
	public String getColumnSeparator() {
		return columnSeparator;
	}

	public void setColumnSeparator(String columnSeparator) {
		this.columnSeparator = columnSeparator;
	}

	public String getNullValue() {
		return nullValue;
	}

	public void setNullValue(String nullValue) {
		this.nullValue = nullValue;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}
	
	public int getSkipLines() {
		return skipLines;
	}

	public void setSkipLines(int skipLines) {
		this.skipLines = skipLines;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
	
	public TxtDeserializer columnSeparator(String columnSeparator) {
		setColumnSeparator(columnSeparator);
		return this;
	}
	
	public TxtDeserializer nullValue(String nullValue) {
		setNullValue(nullValue);
		return this;
	}
	
	public TxtDeserializer columns(String columns) {
		setColumns(columns);
		return this;
	}
	
	public TxtDeserializer skipLines(int skipLines) {
		setSkipLines(skipLines);
		return this;
	}
	
	public TxtDeserializer charset(String charset) {
		setCharset(charset);
		return this;
	}
	
	private Map toMap(String line,String[] columnNames) {
		String[] columnValues = splitLine(line,columnSeparator);
		return MapUtil.toMap(columnValues, columnNames);
	}
	
	private String[] splitLine(String line, String columnSeparator) {
		String[] array = org.apache.commons.lang.StringUtils.splitPreserveAllTokens(line, columnSeparator);
		for (int i = 0; i < array.length; i++) {
			if (nullValue.equals(array[i])) {
				array[i] = null;
			}
		}
		return array;
	}

	public Map read(BufferedReader reader) {
		try {
			String line = reader.readLine();
			if(line == null) {
				return null;
			}
			if(StringUtils.isBlank(line)) {
				return read(reader);
			}
			if(line.startsWith("#")) {
				return read(reader);
			}
			return toMap(line, columnNames);
		}catch(Exception e) {
			throw new RuntimeException("read() error,id:"+getId(),e);
		}
	}

	private void init() {
		Assert.hasText(columns,"columns must be not empty");
		columnNames = Util.splitColumns(columns);
	}

	@Override
	public Map deserialize(InputStream inputStream) throws IOException {
		if(!isInit) {
			isInit = true;
			init();
		}
		BufferedReader in = cache.get(inputStream);
		if(in == null) {
			synchronized (cache) {
				String defaultCharset = Charset.defaultCharset().name();
				in = new BufferedReader(new InputStreamReader(inputStream,StringUtils.defaultIfEmpty(charset, defaultCharset)));
				execSkipLines(in,skipLines);
				cache .put(inputStream,in);
			}
		}
		return read(in);
	}

	private void execSkipLines(BufferedReader in, int skipLines) throws IOException {
		if(skipLines <= 0) return;
		
		for(int i = 0; i < skipLines; i++) {
			String skipedLine = in.readLine();
			if(skipedLine == null) {
				return;
			}
		}
	}

}
