package com.github.dataswitch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class VTest {

	volatile static int count = 0;
	static AtomicLong acount = new AtomicLong();
	
	@Test
	public void testArray() {
		List list = new ArrayList();
		list.forEach((item) -> {
			
		});
	}
	
	public static void main(String[] args) throws InterruptedException {
		final int loopCount = 1000;
		final int threadCount = 100;
		final CountDownLatch countDown = new CountDownLatch(threadCount);
		for(int i = 0; i < threadCount; i++) {
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					for(int i = 0; i < loopCount; i++) {
						count++;
						acount.incrementAndGet();
					}
					countDown.countDown();
				}
			});
			t.start();
		}
		countDown.await();
//		Thread.sleep(1000);
		System.out.println(count);
		System.out.println(acount);
	}
	
}
