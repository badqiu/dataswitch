package com.github.dataswitch.input;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.Openable;

public interface Input extends Openable,Closeable{
	
	/**
	 * 读取数据，
	 * @param size 每次读取批量大小
	 * @return 返回为null或空，代表读取数据结束，程序可以退出
	 */
	public List<Object> read(int size) ;
	
	public default void commitInput() {};
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
	}
	
	@Override
	public default void close() throws IOException {
	}
}
