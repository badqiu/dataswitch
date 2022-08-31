package com.github.dataswitch.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.springframework.util.Assert;

import com.github.dataswitch.Openable;

public class QueueProvider implements Openable {
	private int queueSize = 100;
	
	private String queueGroup = "default";
	private String queueName;
	private BlockingQueue<List<Object>> queue = null;
	
	public void init() {
		
		if(queue == null) {
			Assert.hasText(queueGroup,"queueGroup must be not blank");
			Assert.hasText(queueName,"queueName must be not blank");
			String queueId = queueGroup + "|" + queueName;
			queue = newBlockingQueue(queueId,queueSize);
		}
		
	}
	
	public BlockingQueue newBlockingQueue(String queueId,int queueSize) {
		return QueueUtil.getBlockingQueue(queueId, queueSize);
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		init();
	}

	public BlockingQueue<List<Object>> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<List<Object>> queue) {
		this.queue = queue;
	}
	
}
