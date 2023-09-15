package com.github.dataswitch.input;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.util.InputOutputUtil;
import com.github.dataswitch.util.Util;
/**
 * 通过线程异步读取数据的Input 
 * 
 * @author badqiu
 *
 */
public class AsyncInput extends ProxyInput{
	private static Logger logger = LoggerFactory.getLogger(AsyncInput.class);
	
	private BlockingQueue<List> queue = new ArrayBlockingQueue<List>(100);

	private boolean running = true;
	private Exception lastException;
	private Object lastExceptionData;
	private Thread thread = null;
	private FailMode failMode = FailMode.FAIL_FAST;
	private int readSize = Constants.DEFAULT_BUFFER_SIZE;
	
	private Consumer<Exception> exceptionHandler = null;
	
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
	
	public Consumer<Exception> getExceptionHandler() {
		return exceptionHandler;
	}

	public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public List<Object> read(int size) {
		readSize = size;
		
		if(FailMode.FAIL_FAST == failMode && lastException != null) {
			throw new RuntimeException("has exception" + lastException+" lastExceptionData:"+Util.first(lastExceptionData),lastException);
		}
		
		try {
			if(!running) {
				if(!queue.isEmpty()) {
					return queue.take();
				}
			}
			
			
			List rows = queue.take();
			
			if(CollectionUtils.isEmpty(rows)) {
				//exit sign
				
			}
			
			
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
	
	protected void startReadThread() {
		Input input = getProxy();
		
		String threadName = getClass().getSimpleName()+"_read_"+getId();
		Runnable readRunnable = newReadRunnable(input, threadName);
		thread = new Thread(readRunnable,threadName);
		thread.setDaemon(true);
		thread.start();
	}

	protected Runnable newReadRunnable(Input input, String threadName) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					logger.info(threadName+" started");
					
					while(running) {
						List rows = null;
						try {
							rows = input.read(readSize);
							if(CollectionUtils.isEmpty(rows)) {
								running = false;
								List exitSign = Collections.EMPTY_LIST;
								queuePutAndCommitInput(input,exitSign);
								return; //exit for no data
							}
							
							queuePutAndCommitInput(input, rows);
						}catch(InterruptedException e) {
							logger.info("InterruptedException on read thread,exit thread",e);
							return;
						}catch(Exception e) {
							Object firstRow = Util.first(rows);
							logger.warn("ignore error on read thread, one dataRow:"+firstRow,e);
							lastException = e;
							lastExceptionData = firstRow;
							
							if(exceptionHandler != null) {
								exceptionHandler.accept(e);
							}
						}
					}
				}finally {
					InputOutputUtil.closeQuietly(input);
					logger.info("exit read thread,threadName:"+threadName);
				}
			}

			private void queuePutAndCommitInput(Input input, List rows) throws InterruptedException {
				queue.put(rows);
				input.commitInput();
			}


		};
	}
	
	@Override
	public void close() throws Exception  {
		
		running = false;
		
		if(thread != null) {
			thread.interrupt();
			thread.join();
		}
		
		super.close();
		
		failMode.throwExceptionIfFailAtEnd(lastException, lastExceptionData);
		
	}
}
