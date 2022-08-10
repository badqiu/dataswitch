package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
/**
 * 提供缓冲功能的Output,缓冲池大小根据bufSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static int DEFAULT_BUF_SIZE = 2000;
	private int bufSize;
	private int bufTimeout;
	private long lastSendTime;
	private List<Object> buf = new ArrayList<Object>();
	
	public BufferedOutput(Output proxy) {
		this(proxy,DEFAULT_BUF_SIZE,0);
	}
	
	public BufferedOutput(Output proxy,int bufSize) {
		this(proxy,bufSize,0);
	}
	
	public BufferedOutput(Output proxy,int bufSize,int bufTimeout) {
		super(proxy);
		if(bufSize <= 0) {
			throw new IllegalArgumentException("bufSize > 0 must be true");
		}
		this.bufSize = bufSize;
		this.bufTimeout = bufTimeout;
		buf = new ArrayList<Object>(bufSize);
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		buf.addAll(rows);

		if(buf.size() > bufSize) {
			flushBuffer();
		}else if(bufTimeout > 0 && isTimeout()) {
			flushBuffer();
		}
	}

	private boolean isTimeout() {
		return lastSendTime - System.currentTimeMillis() > bufTimeout;
	}

	public void flushBuffer() {
		if(buf == null || buf.isEmpty()) {
			return;
		}
		if(bufTimeout > 0) {
			lastSendTime = System.currentTimeMillis();
		}
		List<Object> tempBuf = buf;
		buf = new ArrayList<Object>(bufSize);
		super.write(tempBuf);
	}
	
	@Override
	public void close() {
		flushBuffer();
		super.close();
	}

}
