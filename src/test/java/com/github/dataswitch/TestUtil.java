package com.github.dataswitch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.github.dataswitch.input.Input;

public class TestUtil {

	public static int printInputReadRows(Input input) throws Exception {
		return printInputReadRows(input,1);
	}
	
	public static int printInputReadRows(Input input,int expectedSize) throws Exception {
		boolean empty = true;
		int count = 0;
		for(int i = 0; i < 10000; i++) {
			List<Map<String,Object>> rows = input.read(1);
			if(!rows.isEmpty()) {
				empty = false;
				if(expectedSize > 0)
					Assert.assertEquals(expectedSize,rows.size());
			}
			printRows(rows);
			count += rows.size();
		}
		
		Assert.assertFalse("must be not empty",empty);
		input.close();
		return count;
	}
	
	public static void printRows(List<? extends Object> rows) {
		for(Object row : rows) {
			System.out.println(row);
		}
	}
	
	public static List<Map<String,Object>> newTestDatas(int size) {
		List<Map<String,Object>> rows = new ArrayList<Map<String,Object>>();
		for(int i = 0; i < size;i++) {
			rows.add(newMap("id",i,"username","badqiu_"+i,"pwd","123","age",20+i));
		}
		return rows;
	}
	
	public static Map newMap(Object... v) {
		Map map = new HashMap();
		for(int i = 0;i < v.length; i+=2) {
			map.put(v[i], v[i+1]);
		}
		return map;
	}
	
}
