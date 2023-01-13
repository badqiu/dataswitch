package com.github.dataswitch.processor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.dataswitch.util.MapUtil;

public class WhereProcessorTest {

	SqlFuncProcessor p = new SqlFuncProcessor();
	@Test
	public void test_limit() throws Exception {
		List rows = newList(10);
		
		p.setLimit(5);
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0}, {count=1}, {count=2}, {count=3}, {count=4}]",results.toString());
	}
	
	@Test
	public void test_offset() throws Exception {
		List rows = newList(10);
		List results = null;
		
		p = new SqlFuncProcessor();
		p.setLimit(5);
		p.setOffset(1);
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=1}, {count=2}, {count=3}, {count=4}, {count=5}]",results.toString());
		
		p = new SqlFuncProcessor();
		p.setLimit(Integer.MAX_VALUE);
		p.setOffset(8);
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=8}, {count=9}]",results.toString());
		
		p = new SqlFuncProcessor();
		p.setLimit(Integer.MAX_VALUE);
		p.setOffset(0);
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0}, {count=1}, {count=2}, {count=3}, {count=4}, {count=5}, {count=6}, {count=7}, {count=8}, {count=9}]",results.toString());
	}
	
	@Test
	public void test_select() throws Exception {
		List rows = newList(10);
		
		p.setSelect("name");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{}, {}, {}, {}, {}, {}, {}, {}, {}, {}]",results.toString());
		
		p.setSelect("count,name");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0}, {count=1}, {count=2}, {count=3}, {count=4}, {count=5}, {count=6}, {count=7}, {count=8}, {count=9}]",results.toString());
	}
	
	@Test
	public void test_remove() throws Exception {
		List rows = newList(10);
		
		p.setRemove("name");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0}, {count=1}, {count=2}, {count=3}, {count=4}, {count=5}, {count=6}, {count=7}, {count=8}, {count=9}]",results.toString());

		p.setRemove("count");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{}, {}, {}, {}, {}, {}, {}, {}, {}, {}]",results.toString());
	
	}
	
	@Test
	public void test_where() throws Exception {
		List rows = newList(10);
		
		p.setWhere("1 == 1 and 1 != 0");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0}, {count=1}, {count=2}, {count=3}, {count=4}, {count=5}, {count=6}, {count=7}, {count=8}, {count=9}]",results.toString());

		p.setWhere("count > 5");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=6}, {count=7}, {count=8}, {count=9}]",results.toString());
	
	}
	
	public List<Map> newList(int count) {
		List r = new ArrayList();
		for(int i = 0; i < count; i++) {
			r.add(MapUtil.newMap("count",i));
		}
		return r;
	}

}
