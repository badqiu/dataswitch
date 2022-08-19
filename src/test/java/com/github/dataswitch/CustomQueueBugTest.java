package com.github.dataswitch;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.github.rapid.common.test.util.MultiThreadTestUtils;
import com.github.rapid.common.util.ThreadUtil;

public class CustomQueueBugTest {
	private static Map<String,BlockingQueue> queueMap = new HashMap(50);
	private static int DEFAULT_QUEUE_SIZE = 10000;
	
	public static BlockingQueue getQueue(String queueName,Class<Queue> queueClass) {
		return getQueue(queueName,queueClass,DEFAULT_QUEUE_SIZE);
	}
	
	public static BlockingQueue getQueue(String queueName,Class<Queue> queueClass,int queueSize) {
		BlockingQueue queue = queueMap.get(queueName);
		if(queue == null) {
			synchronized (queueMap) {
				queue = queueMap.get(queueName);
				if(queue == null) {
					queue = new ArrayBlockingQueue(queueSize);
					queueMap.put(queueName, queue);
				}
			}
		}
		return queue;
	}
	
	
	@Test
	public void testGetQueue() throws InterruptedException {
		BlockingQueue queue = getQueue("testQueue",null);
		
		MultiThreadTestUtils.execute(10, () -> {
			for(int index = 0; index < 10; index++){
				String name = Thread.currentThread().getName();
				String string = name +" - " + index;
				System.out.println("offer:"+string);
				ThreadUtil.sleep(100);
				queue.offer(string);
			}
		});
		
		for(int i = 0; i < 500; i++) {
			System.out.println("queue.take() "+queue.take());
		}
	}
	
}
