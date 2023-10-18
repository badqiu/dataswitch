package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.github.dataswitch.Enabled;
import com.github.dataswitch.Openable;

public interface Input extends Function<Integer, List<Map<String, Object>>>,Openable,AutoCloseable{
	
	/**
	 * 读取数据，
	 * @param size 每次读取批量大小
	 * @return 返回为null或空，代表读取数据结束，程序可以退出
	 */
	public List<Map<String, Object>> read(int size) ;
	
	/** 一般output处理完数据，用于kafka的offset commit */
	public default void commitInput() {};
	
	@Override
	public default void open(Map<String, Object> params) throws Exception {
		Enabled.assertEnabled(this);
	}
	
	@Override
	public default void close() throws Exception {
	}
	
	@Override
	default List<Map<String, Object>> apply(Integer t) {
		return read(t);
	}
}
