package com.github.dataswitch.util.hadoop;
/**
 * @author badqiu
 */
public class HdfsIOException extends RuntimeException{

	public HdfsIOException() {
		super();
	}

	public HdfsIOException(String message, Throwable cause) {
		super(message, cause);
	}

	public HdfsIOException(String message) {
		super(message);
	}

	public HdfsIOException(Throwable cause) {
		super(cause);
	}
	
}
