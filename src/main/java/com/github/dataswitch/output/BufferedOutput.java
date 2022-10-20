package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rapid.common.util.SystemTimer;

/**
 * 提供缓冲功能的Output,缓冲池大小根据bufferSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static Logger logger = LoggerFactory.getLogger(BufferedOutput.class);
	
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
		return Math.abs(lastSendTime - SystemTimer.currentTimeMillis()) > bufferTimeout;
	}
	
	@Override
	public void flush()  {
		flushBuffer();
	}

	public void flushBuffer() {
		if(bufferList == null || bufferList.isEmpty()) {
			return;
		}
		if(bufferTimeout > 0) {
			lastSendTime = SystemTimer.currentTimeMillis();
		}
		List<Object> tempBuf = bufferList;
		bufferList = new ArrayList<Object>(bufferSize);
		super.write(tempBuf);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		init();
	}
	
	private void init() {
		startAutoFlushThread();
	}

	private void startAutoFlushThread() {
		if(bufferTimeout <= 0) {
			return;
		}
		
		String threadName = getClass().getSimpleName() + "_auto_flush";
		
		Thread t = new Thread(() -> {
			
			logger.info("flush thread started,bufferTimeout:"+bufferTimeout);
			try {
				while(true) {
					try {
						Thread.sleep(bufferTimeout);
					} catch (InterruptedException e) {
						return;
					}
					
					try {
						flush();
					}catch(Exception e) {
						logger.error("flush error",e);
					}
				}
			}finally {
				logger.info("flush thread exit");
			}
			
		},threadName);
		
		t.start();
	}

	@Override
	public void close() throws Exception {
		flush();
		super.close();
	}

}
