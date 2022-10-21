package com.github.dataswitch.util;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

public class BlockingQueueTest {
	private BlockingQueue<List> queue = new ArrayBlockingQueue<List>(100);
	
	@Test
	public void test_put_null() throws InterruptedException {
		
		queue.put(null);
	}
}
