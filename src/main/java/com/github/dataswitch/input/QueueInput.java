package com.github.dataswitch.input;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.github.dataswitch.output.QueueOutput;
import com.github.dataswitch.support.QueueProvider;

public class QueueInput extends QueueProvider implements Input{

	public QueueInput() {
		super();
	}

	public QueueInput(BlockingQueue<List<Object>> queue) {
		super(queue);
	}

	public QueueInput(String queueGroup, String queueName) {
		super(queueGroup, queueName);
	}

	@Override
	public List<Object> read(int size) {
		try {
			return getQueue().take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private QueueOutput _output = null;
	
	public synchronized QueueOutput toOutput() {
		if(_output == null) {
			_output = new QueueOutput(getQueue());
			try {
				_output.open(null);
			} catch (Exception e) {
				throw new RuntimeException("open error",e);
			}
		}
		return _output;
	}
}
