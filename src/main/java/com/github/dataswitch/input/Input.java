package com.github.dataswitch.input;

import java.io.Closeable;
import java.util.List;

public interface Input extends Closeable{
	
	/**
	 * 读取数据，
	 * @param size 每次读取批量大小
	 * @return 返回为null或空，代表读取数据结束，程序可以退出
	 */
	public List<Object> read(int size) ;
	
	public default void commitInput() {};
}
