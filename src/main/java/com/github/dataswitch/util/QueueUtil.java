package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

import com.google.common.collect.Queues;

public class QueueUtil {

	private static Map<String,BlockingQueue> queueMap = new HashMap<String,BlockingQueue>();
	
	public static synchronized BlockingQueue getBlockingQueue(String queueId,int queueSize) {
		Assert.hasText(queueId,"queueId must be not blank");
		Assert.isTrue(queueSize > 0,"queueSize > 0 must be true");
		
		BlockingQueue queue = queueMap.get(queueId);
		if(queue == null) {
			queue = new ArrayBlockingQueue(queueSize);
			queueMap.put(queueId, queue);
		}
		return queue;
	}
	
	public static <T> List<T> batchTake(BlockingQueue<T> queue,int size, int timeout) throws InterruptedException {
		List results = new ArrayList(size);
		long startReadTime = System.currentTimeMillis();
		
		for(int i = 0; i < size; i++) {
			Object object = queue.take();
			results.add(object);
			if(isTimeout(timeout,startReadTime)) {
				break;
			}
		}
		
		return results;
		
//		Queues.drain(queue, results, size, timeout, TimeUnit.MILLISECONDS);
//		return results;
	}
	
	private static boolean isTimeout(int timeout,long startReadTime) {
		long interval = System.currentTimeMillis() - startReadTime;
		return interval > timeout;
	}
	
}
