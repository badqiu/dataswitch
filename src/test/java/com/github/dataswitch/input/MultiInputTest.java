package com.github.dataswitch.input;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.dataswitch.TestUtil;
import com.github.dataswitch.input.FileInput;
import com.github.dataswitch.input.MultiInput;

public class MultiInputTest {

	@Test
	public void test_multi() throws Exception {
		FileInput fileInput1 = FileInputTest.newFileInput("name,age , sex", "classpath:test/fileinput/abc");
		FileInput fileInput2 = FileInputTest.newFileInput("name,age , sex", "classpath:test/fileinput/2.txt");
		MultiInput mi = new MultiInput(fileInput1,fileInput2);
		mi.open(null);
		int rows = TestUtil.printInputReadRows(mi);
		assertEquals(rows,6);
	}
	
	@Test
	public void test_multi_by_setConcurrentRead() throws Exception {
		FileInput fileInput1 = FileInputTest.newFileInput("name,age , sex", "classpath:test/fileinput/abc");
		FileInput fileInput2 = FileInputTest.newFileInput("name,age , sex", "classpath:test/fileinput/2.txt");
		MultiInput mi = new MultiInput(fileInput1,fileInput2);
		mi.setConcurrent(true);
		mi.setExecutorName("executor1");
		mi.open(null);
//		Thread.sleep(1000);
		
		int rows = TestUtil.printInputReadRows(mi,0);
		
		assertEquals(rows,6);
	}
	
	@Test
	public void test_multi_by_setConcurrentRead_same_executor_name() throws Exception {
		Input input1 = new DataGenInput(5);
		Input input2 = new DataGenInput(3);
		Input input3 = new DataGenInput(10);
		MultiInput mi = new MultiInput(input1,input2,input3);
		mi.setConcurrent(true);
		mi.setExecutorName("executor1");
		mi.open(null);
		
		int rows = TestUtil.printInputReadRows(mi,0);
		
		assertEquals(rows,18);
	}
	
	@Test
	public void test_multi_by_setConcurrentRead2() throws Exception {
		Input input1 = new DataGenInput(5);
		Input input2 = new DataGenInput(3);
		Input input3 = new DataGenInput(10);
		MultiInput mi = new MultiInput(input1,input2,input3);
		mi.setConcurrent(true);
		mi.setExecutorName("executor2");
		mi.open(null);
		
		int rows = TestUtil.printInputReadRows(mi,0);
		
		assertEquals(rows,18);
	}
	
	@Test
	public void test() throws Exception {
		FileInput fileInput1 = FileInputTest.newFileInput("name,age , sex", "classpath:test/fileinput/abc");
		MultiInput mi = new MultiInput(fileInput1);
		mi.open(null);
		int rows = TestUtil.printInputReadRows(mi);
		assertEquals(rows,3);
	}

}
