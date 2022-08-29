package com.github.dataswitch.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
/**
 * 提供缓冲功能的Output,缓冲池大小根据bufferSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static int DEFAULT_BUF_SIZE = 2000;
	
	private int bufferSize;
	private int bufferTimeout;
	
	private long lastSendTime = System.currentTimeMillis();
	private List<Object> bufferList = new ArrayList<Object>();
	
	public BufferedOutput(Output proxy) {
		this(proxy,DEFAULT_BUF_SIZE,0);
	}
	
	public BufferedOutput(Output proxy,int bufferSize) {
		this(proxy,bufferSize,0);
	}
	
	public BufferedOutput(Output proxy,int bufferSize,int bufTimeout) {
		super(proxy);
		if(bufferSize <= 0) {
			throw new IllegalArgumentException("bufferSize > 0 must be true");
		}
		this.bufferSize = bufferSize;
		this.bufferTimeout = bufTimeout;
		bufferList = new ArrayList<Object>(bufferSize);
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		bufferList.addAll(rows);

		if(bufferList.size() > bufferSize) {
			flushBuffer();
		}else if(bufferTimeout > 0 && isTimeout()) {
			flushBuffer();
		}
	}

	private boolean isTimeout() {
		return Math.abs(lastSendTime - System.currentTimeMillis()) > bufferTimeout;
	}
	
	@Override
	public void flush() throws IOException {
		flushBuffer();
	}

	public void flushBuffer() {
		if(bufferList == null || bufferList.isEmpty()) {
			return;
		}
		if(bufferTimeout > 0) {
			lastSendTime = System.currentTimeMillis();
		}
		List<Object> tempBuf = bufferList;
		bufferList = new ArrayList<Object>(bufferSize);
		super.write(tempBuf);
	}
	
	@Override
	public void close() throws IOException {
		flush();
		super.close();
	}

}
