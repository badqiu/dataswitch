package com.github.dataswitch;

import java.io.IOException;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.Test;

public class VTest {

	volatile static int count = 0;
	static AtomicLong acount = new AtomicLong();
	
	@Test
	public void test_device() throws IOException {
		DecimalFormat df = new DecimalFormat("##.##");
		double r = Double.MAX_VALUE;
		for(int i = 0; i < 15; i++) {
			r = r / Long.MAX_VALUE ;
		}
		
		System.out.println("double/long,value: "+df.format(r));
		System.out.println("Long.MAX_VALUE:\t\t"+df.format(Long.MAX_VALUE));
		System.out.println("Double.MAX_VALUE:\t"+df.format(Double.MAX_VALUE));
		System.out.println("Float.MAX_VALUE: "+df.format(Float.MAX_VALUE));
		System.out.println("double/long,value: "+df.format(Double.MAX_VALUE/Long.MAX_VALUE));
		System.out.println("double/float,value: "+df.format(Double.MAX_VALUE/Float.MAX_VALUE));
		System.out.println("BigInteger,value: "+df.format(new BigInteger("1797693134862315700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000022222200000000000000000000000000000000000000000000000000000000000000000001111")));
		
		String id = genId();
		System.out.println(id);
		System.out.println(df.format(new BigInteger(id)));
	}
	
	long seq = 0;
	String workId = "99900001";
	private String genId() {
		String time = DateFormatUtils.format(new Date(), "yyyyMMddHHmmssSSS");
		long incr = ++seq;
		return time+workId+incr;
	}

	@Test
	public void test_exec() throws IOException {
		Process p1 = Runtime.getRuntime().exec("bin/flume-ng agent --name usbyteplus1 --conf conf/ --conf-file conf/usbyteplus1.conf -Dflume.root.logger=INFO");
		IOUtils.copy(p1.getInputStream(),System.out);
	}
	
	@Test
	public void testArray() {
		List list = new ArrayList();
		list.forEach((item) -> {
			
		});
	}
	
	@Test
	public void testUUID() {
		System.out.println(UUID.randomUUID().toString());
		System.out.println(System.currentTimeMillis());
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
