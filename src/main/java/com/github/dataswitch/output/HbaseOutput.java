package com.github.dataswitch.output;

public class HbaseOutput {
	private String hbaseConfig;
	
	private String columnFamily;
	private String columns;
	private String rowkeyColumn;
	private String versionColumn;
	
	private String table;
	private String encoding;
	
	private String properties;
	
	private boolean walFlag; //关闭WAL日志写入，可以提升性能，但存在数据丢失风险
	private int writeBufferSize;
	
}
