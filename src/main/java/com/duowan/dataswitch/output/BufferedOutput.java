package com.duowan.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
/**
 * 提供缓冲功能的Output,缓冲池大小根据bufSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static int DEFAULT_BUF_SIZE = 2000;
	private int bufSize;
	private List<Object> buf = new ArrayList<Object>();
	
	public BufferedOutput(Output proxy) {
		this(proxy,DEFAULT_BUF_SIZE);
	}
	
	public BufferedOutput(Output proxy,int bufSize) {
		super(proxy);
		if(bufSize <= 0) {
			throw new IllegalArgumentException("bufSize > 0 must be true");
		}
		this.bufSize = bufSize;
		buf = new ArrayList<Object>(bufSize);
	}
	
	@Override
	public void write(List<Object> rows) {
		buf.addAll(rows);
		if(buf.size() > bufSize) {
			List<Object> tempBuf = buf;
			buf = new ArrayList<Object>(bufSize);
			super.write(tempBuf);
		}
	}

}
