package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.ThreadUtil;
import com.github.dataswitch.util.Util;

/**
 * 异步output
 * @author badqiu
 *
 */
public class AsyncOutput extends ProxyOutput{
	private static Logger logger = LoggerFactory.getLogger(AsyncOutput.class);
	
	private BlockingQueue<List> queue = new ArrayBlockingQueue<List>(100);

	private boolean running = true;
	private Exception lastException;
	private Object lastExceptionData;
	private Thread thread = null;
	private FailMode failMode = FailMode.FAIL_FAST;
	
	public AsyncOutput() {
		super();
	}

	public AsyncOutput(Output proxy) {
		super(proxy);
	}
	
	public BlockingQueue<List> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<List> queue) {
		this.queue = queue;
	}

	@Override
	public void write(List<Object> rows)  {
		if(CollectionUtils.isEmpty(rows)) return;
		
		if(FailMode.FAIL_FAST == failMode && lastException != null) {
			throw new RuntimeException("has exception" + lastException+" lastExceptionData:"+Util.first(lastExceptionData),lastException);
		}
		
		try {
			queue.put(rows);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		
		startWriteThread();
	}

	private void startWriteThread() {
		Output output = getProxy();
		
		String threadName = getClass().getSimpleName()+"_write_"+getId();
		thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					
					while(running) {
						List rows = null;
						try {
							rows = queue.take();
							if(CollectionUtils.isEmpty(rows)) {
								continue;
							}
							
							output.write(rows);
						}catch(InterruptedException e) {
							logger.info("InterruptedException on write thread,exit thread",e);
							return;
						}catch(Exception e) {
							Object firstRow = CollectionUtils.get(rows, 0);
							logger.warn("ignore error on write thread, one dataRow:"+firstRow,e);
							lastException = e;
							lastExceptionData = firstRow;
						}
					}
				}finally {
					InputOutputUtil.closeQuietly(output);
					logger.info("exit write thread,threadName:"+threadName);
				}
			}


		},threadName);
		
		thread.setDaemon(true);
		thread.start();
	}
	
	@Override
	public void close() throws Exception {
		
		waitQueueIsEmpty();
		
		running = false;
		
		if(thread != null) {
			thread.interrupt();
			thread.join();
		}
		
		super.close();
		
		failMode.throwExceptionIfFailAtEnd(lastException, lastExceptionData);
		
	}

	private void waitQueueIsEmpty() {
		while(!queue.isEmpty()) {
			ThreadUtil.sleep(1000);
		}
	}
	
}
