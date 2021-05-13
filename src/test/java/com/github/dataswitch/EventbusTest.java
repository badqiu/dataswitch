package com.github.dataswitch;

import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class EventbusTest {

	@Test
	public void test() throws InterruptedException {
//		EventBus eventbus = new EventBus();
		EventBus eventbus = new AsyncEventBus(Executors.newFixedThreadPool(2));
		eventbus.register(new NumListener());
		
		for(int i = 0; i < 100; i++) {
			eventbus.post(new Long(i));
			eventbus.post(new Integer(i));
		}
		
		Thread.sleep(500);
	}
	
	public static class NumListener {
		
		@Subscribe
		public void listenLong(Long v) {
			System.out.println(Thread.currentThread().getId() + " listenLong,v="+v);
		}
		
		@Subscribe
		public void listenInteger(Integer v) {
			System.out.println(Thread.currentThread().getId() + " listenInteger,v="+v);
		}
		
	}
	
	@Test
	public void test_sqrt() {
		System.out.println(""+Math.sqrt(150));
		System.out.println(""+Math.sqrt(230));
		System.out.println(""+Math.sqrt(500));
		System.out.println(""+Math.sqrt(1000));
		System.out.println("1111");
	}
}
