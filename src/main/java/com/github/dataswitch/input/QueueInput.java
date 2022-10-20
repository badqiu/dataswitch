package com.github.dataswitch.input;

import java.util.List;

import com.github.dataswitch.support.QueueProvider;

public class QueueInput extends QueueProvider implements Input{

	@Override
	public List<Object> read(int size) {
		try {
			return getQueue().take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
