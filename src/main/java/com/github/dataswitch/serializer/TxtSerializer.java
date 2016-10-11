package com.github.dataswitch.serializer;

import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Constants;
import com.github.dataswitch.util.Util;
import com.github.rapid.common.beanutils.PropertyUtils;
import com.github.rapid.common.util.DateConvertUtil;

public class TxtSerializer extends BaseObject implements Serializer<Object>,Flushable{

	private static Logger log = LoggerFactory.getLogger(TxtSerializer.class);
	private String nullValue = Constants.NULL_VALUE;
	private String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	private String columns; // 输出列
	private String columnSplit = Constants.COLUMN_SPLIT; //列分隔符
	private String lineSplit = "\n"; //行分隔符
	private String charset;
	
	private String[] columnNames;
	private boolean isInit = false;
	
	public TxtSerializer(){
	}
	
	public TxtSerializer(TxtSerializer so) {
		setLineSplit(so.getLineSplit());
		setNullValue(so.getNullValue());
		setColumns(so.getColumns());
		setColumnSplit(so.getColumnSplit());
		setCharset(so.getCharset());
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

	public String getColumnSplit() {
		return columnSplit;
	}

	public void setColumnSplit(String columnSplit) {
		this.columnSplit = columnSplit;
	}

	public String getLineSplit() {
		return lineSplit;
	}

	public void setLineSplit(String lineSplit) {
		this.lineSplit = lineSplit;
	}
	
	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
	
	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

//	public void write(Writer out,Object row) {
//		try {
//			List<String> values = new ArrayList<String>();
//			for(String name : columnNames) {
//				Object value = getValue(row, name);
//				values.add(format(value));
//			}
//			out.write(StringUtils.join(values,columnSplit));
//			out.write(lineSplit);
//		}catch(IOException e) {
//			throw new RuntimeException("write() error,id:"+getId(),e);
//		}
//	}
	
	public void write(OutputStream out,Object row) {
		try {
			String str = toLineString(row);
			out.write(charset == null ? str.getBytes() : str.getBytes(charset));
			out.write(lineSplit.getBytes());
		}catch(IOException e) {
			throw new RuntimeException("write() error,id:"+getId(),e);
		}
	}

	private String toLineString(Object row) {
		List<String> values = new ArrayList<String>();
		for(String name : columnNames) {
			Object value = getValue(row, name);
			values.add(format(value));
		}
		return StringUtils.join(values,columnSplit);
	}

	private Object getValue(Object row, String name) {
		if(row instanceof Map) {
			Map map = (Map)row;
			if(map.containsKey(name)) {
				return map.get(name);
			}
			throw new RuntimeException("not exist key:"+name+" on row:"+row);
		}else {
			if(PropertyUtils.isReadable(row, name)) {
				return PropertyUtils.getSimpleProperty(row,name); //TODO 性能是否有问题
			}
			throw new RuntimeException("not exist key:"+name+" on row:"+row);
		}
	}

	private String format(Object value) {
		if(value == null) {
			return nullValue;
		}
		if(value instanceof Date) {
			return DateConvertUtil.format((Date)value, dateFormat);
		}
		return value.toString();
	}


	private Map<OutputStream,Writer> cache = new HashMap<OutputStream,Writer>();

	private void init() {
		Assert.hasText(columns,"columns must be not empty");
		columnNames = Util.splitColumns(columns);
	}

	@Override
	public void serialize(Object object, OutputStream outputStream) throws IOException {
		if(!isInit ) {
			isInit = true;
			init();
		}
		
//		Writer out = cache.get(outputStream);
//		if(out == null) {
//			synchronized (cache) {
//				if(StringUtils.isBlank(charset)) {
//					out = new OutputStreamWriter(outputStream);
//				}else {
//					out = new OutputStreamWriter(outputStream,charset);
//				}
//				cache.put(outputStream,out);
//			}
//		}
//		write(out,object);
		write(outputStream,object);
	}

	@Override
	public void flush() throws IOException {
//		for(Writer writer : cache.values()) {
//			try {
//				writer.flush();
//			}catch(Exception e) {
//				log.error("flosh error",e);
//				//ignore
//			}
//		}
	}

}
