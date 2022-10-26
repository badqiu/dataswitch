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
		mi.setConcurrentRead(true);
		mi.open(null);
//		Thread.sleep(1000);
		
		int rows = TestUtil.printInputReadRows(mi,0);
		
		assertEquals(rows,6);
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
