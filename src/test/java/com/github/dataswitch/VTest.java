package com.github.dataswitch;

import java.util.concurrent.CountDownLatch;

public class VTest {

	volatile static int count = 0;
	
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
					}
					countDown.countDown();
				}
			});
			t.start();
		}
		countDown.await();
		Thread.sleep(1000);
		System.out.println(count);
	}
	
}
