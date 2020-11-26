package com.github.dataswitch.input;

import java.io.Closeable;
import java.util.List;

public interface Input extends Closeable{
	
	/**
	 * 读取数据，
	 * @param size 每次读取批量大小
	 * @return 返回为null或空，代理读取结束，程序退出
	 */
	public List<Object> read(int size) ;
	
}
