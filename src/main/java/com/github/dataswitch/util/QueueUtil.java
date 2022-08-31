package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.springframework.util.Assert;

public class QueueUtil {

	static Map<String,BlockingQueue> queueMap = new HashMap<String,BlockingQueue>();
	
	public static synchronized BlockingQueue getBlockingQueue(String queueId,int queueSize) {
		Assert.hasText(queueId,"queueId must be not blank");
		BlockingQueue queue = queueMap.get(queueId);
		if(queue == null) {
			queue = new ArrayBlockingQueue<List<Object>>(queueSize);
			queueMap.put(queueId, queue);
		}
		return queue;
	}
	
}
