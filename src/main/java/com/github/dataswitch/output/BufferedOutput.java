package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.Constants;

/**
 * 提供缓冲功能的Output,缓冲池大小根据bufferSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static Logger logger = LoggerFactory.getLogger(BufferedOutput.class);
	
	
	private int bufferSize = Constants.DEFAULT_BUFFER_SIZE;
	private long bufferTimeout = 0;
	
	private long lastSendTime = System.currentTimeMillis();
	private List<Object> bufferList = new ArrayList<Object>();
	
	private boolean init = false;
	
	private boolean running = true;
	
	public BufferedOutput(Output proxy) {
		this(proxy,Constants.DEFAULT_BUFFER_SIZE);
	}
	
	public BufferedOutput(Output proxy,int bufferSize) {
		this(proxy,bufferSize,Constants.DEFAULT_BUFFER_TIMEOUT);
	}
	
	public BufferedOutput(Output proxy,int bufferSize,int bufferTimeout) {
		super(proxy);
		if(bufferSize <= 0) {
			throw new IllegalArgumentException("bufferSize > 0 must be true");
		}
		this.bufferSize = bufferSize;
		this.bufferTimeout = bufferTimeout;
		bufferList = new ArrayList<Object>(bufferSize);
	}
	
	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void setBufferTimeout(int bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
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
	public void flush()  {
		flushBuffer();
	}

	public void flushBuffer() {
		if(bufferList == null || bufferList.isEmpty()) {
			return;
		}
		
		List<Object> tempBuf = bufferList;
		bufferList = new ArrayList<Object>(bufferSize);
		try {
			super.write(tempBuf);
		}finally {
			if(bufferTimeout > 0) {
				lastSendTime = System.currentTimeMillis();
			}
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		init();
	}
	
	private void init() {
		Assert.isTrue(!init,"already init");
		
		running = true;
		startAutoFlushThread();
		init = true;
	}

	private void startAutoFlushThread() {
		if(bufferTimeout <= 0) {
			return;
		}
		
		String threadName = getClass().getSimpleName() + "_auto_flush";
		
		Thread t = new Thread(() -> {
			
			logger.info("flush thread started,bufferTimeout:"+bufferTimeout);
			try {
				while(running) {
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
		running = false;
		
	}

}
