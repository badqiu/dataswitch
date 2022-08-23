package com.github.dataswitch.output;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.Openable;

public interface Output extends Closeable,Flushable,Openable{

	public void write(List<Object> rows);
	
	public default void flush() throws IOException{
	}
	
	default void open(Map<String, Object> params) throws Exception {
	}
	
}

