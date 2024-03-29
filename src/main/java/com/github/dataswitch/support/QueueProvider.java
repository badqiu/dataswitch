package com.github.dataswitch.support;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Openable;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.QueueUtil;
import com.github.dataswitch.util.TableName;

public class QueueProvider extends BaseObject implements Openable,TableName{
	
	private int queueSize = 50000 / Constants.DEFAULT_BATCH_SIZE;
	
	private String queueGroup = "default";
	private String queueName;
	
	private BiFunction<String, Integer, BlockingQueue> newQueueFunction;

	private BlockingQueue<List<Map<String, Object>>> queue = null;
	
	public QueueProvider() {
	}
	
	public QueueProvider(BlockingQueue<List<Map<String, Object>>> queue) {
		this.queue = queue;
	}

	public QueueProvider(String queueGroup, String queueName) {
		this.queueGroup = queueGroup;
		this.queueName = queueName;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		init();
	}
	
	public void init() {
		createQueueIfNull();
	}

	protected void createQueueIfNull() {
		if(queue == null) {
			Assert.hasText(queueGroup,"queueGroup must be not blank");
			Assert.hasText(queueName,"queueName must be not blank");
			String queueId = queueGroup + "|" + queueName;
			queue = newBlockingQueue(queueId,queueSize);
		}
	}
	
	protected BlockingQueue newBlockingQueue(String queueId,int queueSize) {
		if(newQueueFunction == null) {
			return QueueUtil.getBlockingQueue(queueId, queueSize);
		}else {
			return newQueueFunction.apply(queueId, queueSize);
		}
	}
	
	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public String getQueueGroup() {
		return queueGroup;
	}

	public void setQueueGroup(String queueGroup) {
		this.queueGroup = queueGroup;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
	public void setTable(String table) {
		setQueueName(table);
	}
	
	@Override
	public String getTable() {
		return getQueueName();
	}

	public BlockingQueue<List<Map<String, Object>>> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<List<Map<String, Object>>> queue) {
		this.queue = queue;
	}

	public BiFunction<String, Integer, BlockingQueue> getNewQueueFunction() {
		return newQueueFunction;
	}

	public void setNewQueueFunction(BiFunction<String, Integer, BlockingQueue> newQueueFunction) {
		this.newQueueFunction = newQueueFunction;
	}
	
}
