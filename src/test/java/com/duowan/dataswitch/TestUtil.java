package com.duowan.dataswitch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.duowan.dataswitch.input.Input;

public class TestUtil {

	public static int printInputReadRows(Input input) throws IOException {
		boolean empty = true;
		int count = 0;
		for(int i = 0; i < 10000; i++) {
			List<Object> rows = input.read(1);
			if(!rows.isEmpty()) {
				empty = false;
				Assert.assertEquals(rows.size(),1);
			}
			printRows(rows);
			count += rows.size();
		}
		Assert.assertFalse(empty);
		input.close();
		return count;
	}
	
	public static void printRows(List<? extends Object> rows) {
		for(Object row : rows) {
			System.out.println(row);
		}
	}
	
	public static List<Object> newTestDatas(int size) {
		List<Object> rows = new ArrayList<Object>();
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
