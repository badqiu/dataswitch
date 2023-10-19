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
 * 提供缓冲功能的Output,缓冲池大小根据batchSize设置
 * 
 * @author badqiu
 *
 */
public class BufferedOutput extends ProxyOutput{

	private static Logger logger = LoggerFactory.getLogger(BufferedOutput.class);
	
	
	private int batchSize = Constants.DEFAULT_BATCH_SIZE;
	private long batchTimeout = 0;
	
	private long lastSendTime = System.currentTimeMillis();
	private List<Map<String, Object>> bufferList = new ArrayList<Map<String, Object>>();
	
	private boolean init = false;
	
	private volatile boolean running = true;
	
	public BufferedOutput(Output proxy) {
		this(proxy,Constants.DEFAULT_BATCH_SIZE);
	}
	
	public BufferedOutput(Output proxy,int batchSize) {
		this(proxy,batchSize,0);
	}
	
	public BufferedOutput(Output proxy,int batchSize,int batchTimeout) {
		super(proxy);
		if(batchSize <= 0) {
			throw new IllegalArgumentException("batchSize > 0 must be true");
		}
		setBatchSize(batchSize);
		setBatchTimeout(batchTimeout);
		bufferList = new ArrayList<Map<String, Object>>(batchSize);
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		bufferList.addAll(rows);

		if(bufferList.size() > batchSize) {
			flushBuffer();
		}else if(batchTimeout > 0 && isTimeout()) {
			flushBuffer();
		}
	}

	private boolean isTimeout() {
		long interval = Math.abs(lastSendTime - System.currentTimeMillis());
		return interval > batchTimeout;
	}
	
	@Override
	public void flush()  {
		flushBuffer();
	}

	public void flushBuffer() {
		if(bufferList == null || bufferList.isEmpty()) {
			return;
		}
		
		List<Map<String, Object>> tempBuf = bufferList;
		bufferList = new ArrayList<Map<String, Object>>(batchSize);
		try {
			super.write(tempBuf);
		}finally {
			if(batchTimeout > 0) {
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
		if(batchTimeout <= 0) {
			return;
		}
		
		String threadName = getClass().getSimpleName() + "_auto_flush";
		
		Thread t = new Thread(() -> {
			
			logger.info("flush thread started,batchTimeout:"+batchTimeout);
			try {
				while(running) {
					try {
						Thread.sleep(batchTimeout);
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
