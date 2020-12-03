package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class BlockingQueueUtil {

	public static List<Object> batchTake(BlockingQueue<Object> queue,int size, int timeout) throws InterruptedException {
		List<Object> results = new ArrayList(100);
		long startReadTime = System.currentTimeMillis();
		
		for(int i = 0; i < size; i++) {
			Object object = queue.take();
			results.add(object);
			if(isTimeout(timeout,startReadTime)) {
				break;
			}
		}
		
		return results;
	}
	
	private static boolean isTimeout(int timeout,long startReadTime) {
		long interval = System.currentTimeMillis() - startReadTime;
		return interval > timeout;
	}
	
}
