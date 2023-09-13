package com.github.dataswitch.output;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.input.QueueInput;
import com.github.dataswitch.support.QueueProvider;

public class QueueOutput extends QueueProvider implements Output{

	
	
	public QueueOutput() {
		super();
	}

	public QueueOutput(BlockingQueue<List<Object>> queue) {
		super(queue);
	}

	public QueueOutput(String queueGroup, String queueName) {
		super(queueGroup, queueName);
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		try {
			getQueue().put(rows);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private QueueInput _input = null;
	
	public synchronized QueueInput toInput() {
		if(_input == null) {
			_input = new QueueInput(getQueue());
		}
		return _input;
	}
	
}
