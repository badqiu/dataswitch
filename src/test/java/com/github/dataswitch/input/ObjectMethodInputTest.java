package com.github.dataswitch.input;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ObjectMethodInputTest {

	ObjectMethodInput input = new ObjectMethodInput();
	@Test
	public void test_getList() {
		input.setObject(new ObjectMethodInputTest());
		input.setMethod("getList");
		
		List results = input.read(1000);
		for(Object row : results) {
			System.out.println(row);
		}
	}
	
	@Test
	public void test_getListWithArgs() {
		input.setObject(new ObjectMethodInputTest());
		input.setMethod("getListWithArgs");
		input.setArgs(6);
		
		List results = input.read(1000);
		for(Object row : results) {
			System.out.println(row);
		}
	}

	@Test
	public void test_staticMethod() {
		input.setObject(new ObjectMethodInputTest());
		input.setMethod("getStaticList");
		
		List results = input.read(1000);
		for(Object row : results) {
			System.out.println(row);
		}
	}
	
	
	public List getList() {
		return Arrays.asList(1,2,3,4,5);
	}
	
	public List getListWithArgs(int num) {
		return Arrays.asList(1,2,3,4,5,num);
	}
	
	public static List getStaticList() {
		return Arrays.asList(1,2,3,4,5);
	}
}
