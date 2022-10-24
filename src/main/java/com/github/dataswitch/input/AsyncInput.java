package com.github.dataswitch.input;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.util.InputOutputUtil;

public class AsyncInput extends ProxyInput{
	private static Logger logger = LoggerFactory.getLogger(AsyncInput.class);
	
	private BlockingQueue<List> queue = new ArrayBlockingQueue<List>(100);

	private boolean running = true;
	private Exception lastException;
	private Thread thread = null;
	private FailMode failMode = FailMode.FAIL_FAST;
	private int readSize = Constants.DEFAULT_BUFFER_SIZE;
	
	public AsyncInput() {
		super();
	}

	public AsyncInput(Input proxy) {
		super(proxy);
	}
	
	public BlockingQueue<List> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<List> queue) {
		this.queue = queue;
	}

	public FailMode getFailMode() {
		return failMode;
	}

	public void setFailMode(FailMode failMode) {
		this.failMode = failMode;
	}

	@Override
	public List<Object> read(int size) {
		readSize = size;
		
		if(FailMode.FAIL_FAST == failMode && lastException != null) {
			throw new RuntimeException("has exception" + lastException,lastException);
		}
		
		try {
			if(!running) {
				if(!queue.isEmpty()) {
					return queue.take();
				}
			}
			
			
			List rows = queue.take();
			
			return rows;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		startReadThread();
	}
	
	private void startReadThread() {
		Input input = getProxy();
		
		String threadName = getClass().getSimpleName()+"_read_"+getId();
		thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					
					while(running) {
						List rows = null;
						try {
							rows = input.read(readSize);
							if(CollectionUtils.isEmpty(rows)) {
								running = false;
								queue.put(Collections.EMPTY_LIST);
								input.commitInput();
								return; //exit for no data
							}
							
							queue.put(rows);
							
							
							input.commitInput();
						}catch(InterruptedException e) {
							logger.info("InterruptedException on read thread,exit thread",e);
							return;
						}catch(Exception e) {
							Object firstRow = CollectionUtils.get(rows, 0);
							logger.warn("ignore error on read thread, one dataRow:"+firstRow,e);
							lastException = e;
						}
					}
				}finally {
					InputOutputUtil.closeQuietly(input);
					logger.info("exit read thread,threadName:"+threadName);
				}
			}


		},threadName);
		
		thread.setDaemon(true);
		thread.start();
	}
	
	@Override
	public void close() throws Exception  {
		
		running = false;
		
		if(thread != null) {
			thread.interrupt();
			thread.join();
		}
		
		super.close();
	}
}