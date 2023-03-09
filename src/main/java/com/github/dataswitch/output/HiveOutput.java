package com.github.dataswitch.output;

public class HiveOutput {

	private String defaultFS; // hdfs://xxx:port
	private String fileType; //orc,text
	private String path;
	private String fileName;
	private String columns;
	private String writeMode; //append,truncate,nonConflict
	private String compress; //none,gzip,snappy
	private String fieldDelimiter;
	private String hadoopConfig;
	private String encoding = "UTF-8";
	
}
