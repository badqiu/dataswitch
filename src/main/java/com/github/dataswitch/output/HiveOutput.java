package com.github.dataswitch.output;

public class HiveOutput {

	private String defaultFS;
	private String fileType;
	private String path;
	private String fileName;
	private String columns;
	private String writeMode; //append,truncate,nonConflict
	private String compress; //gzip,snappy
}
