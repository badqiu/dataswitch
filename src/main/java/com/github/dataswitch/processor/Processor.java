package com.github.dataswitch.processor;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.Openable;

public interface Processor extends Closeable,Openable{

	public List<Object> process(List<Object> datas) throws Exception;
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
	}
	
	@Override
	public default void close() throws IOException {
	}
}
