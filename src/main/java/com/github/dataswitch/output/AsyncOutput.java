package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

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
	private FailMode failMode = FailMode.FAIL_FAST;
	private Consumer<Exception> exceptionHandler = null;

	private boolean running = true;
	private Exception lastException;
	private Object lastExceptionData;
	
	private Thread _writeThread = null;
	
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
	
	public Consumer<Exception> getExceptionHandler() {
		return exceptionHandler;
	}

	public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
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
		Runnable runnable = new WriteRunnable(output, threadName);
		_writeThread = new Thread(runnable,threadName);
		
		_writeThread.setDaemon(true);
		_writeThread.start();
	}
	
	private class WriteRunnable implements Runnable {
		Output output;
		String threadName;
		
		public WriteRunnable(Output output, String threadName) {
			this.output = output;
			this.threadName = threadName;
		}

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
						logger.info("InterruptedException on write _writeThread,exit _writeThread");
						return;
					}catch(Exception e) {
						Object firstRow = Util.first(rows);
						logger.warn("ignore error on write _writeThread, one dataRow:"+firstRow,e);
						lastException = e;
						lastExceptionData = firstRow;
						
						if(exceptionHandler != null) {
							exceptionHandler.accept(e);
						}
					}
				}
			}finally {
				InputOutputUtil.closeQuietly(output);
				logger.info("exit write _writeThread,threadName:"+threadName);
			}
		}
	}

	
	@Override
	public void close() throws Exception {
		
		waitQueueIsEmpty();
		
		running = false;
		
		if(_writeThread != null) {
			_writeThread.interrupt();
			_writeThread.join();
		}
		
		super.close();
		
		failMode.throwExceptionIfFailAtEnd(lastException, lastExceptionData);
		
	}

	private void waitQueueIsEmpty() {
		while(!queue.isEmpty()) {
			ThreadUtil.sleep(500);
		}
	}
	
}
