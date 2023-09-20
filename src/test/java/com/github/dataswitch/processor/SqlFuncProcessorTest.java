package com.github.dataswitch.processor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import com.github.dataswitch.util.MapUtil;

public class SqlFuncProcessorTest {

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
		List rows = newListWithName(10);
		
		p.setSelect("name");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{name=n10}, {name=n9}, {name=n8}, {name=n7}, {name=n6}, {name=n5}, {name=n4}, {name=n3}, {name=n2}, {name=n1}]",results.toString());
		
		p.setSelect("count,name");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0, name=n10}, {count=1, name=n9}, {count=2, name=n8}, {count=3, name=n7}, {count=4, name=n6}, {count=5, name=n5}, {count=6, name=n4}, {count=7, name=n3}, {count=8, name=n2}, {count=9, name=n1}]",results.toString());
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
	
	@Test
	public void test_orderBy() throws Exception {
		List rows = newListWithName(10);
		
		p.setOrderBy("name");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=9, name=n1}, {count=0, name=n10}, {count=8, name=n2}, {count=7, name=n3}, {count=6, name=n4}, {count=5, name=n5}, {count=4, name=n6}, {count=3, name=n7}, {count=2, name=n8}, {count=1, name=n9}]",results.toString());

		p.setOrderBy("count");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=0, name=n10}, {count=1, name=n9}, {count=2, name=n8}, {count=3, name=n7}, {count=4, name=n6}, {count=5, name=n5}, {count=6, name=n4}, {count=7, name=n3}, {count=8, name=n2}, {count=9, name=n1}]",results.toString());
	
		p.setOrderBy("count desc");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{count=9, name=n1}, {count=8, name=n2}, {count=7, name=n3}, {count=6, name=n4}, {count=5, name=n5}, {count=4, name=n6}, {count=3, name=n7}, {count=2, name=n8}, {count=1, name=n9}, {count=0, name=n10}]",results.toString());
	
	}
	
	@Test
	public void test_groupBy() throws Exception {
		List rows = newListWithName(10);
		
		p.setGroupBy("name");
		p.open(null);
		List results = p.process(rows);
		System.out.println(results);
		assertEquals("[{n1=[{count=9, name=n1}], n2=[{count=8, name=n2}], n3=[{count=7, name=n3}], n4=[{count=6, name=n4}], n5=[{count=5, name=n5}], n6=[{count=4, name=n6}], n7=[{count=3, name=n7}], n8=[{count=2, name=n8}], n9=[{count=1, name=n9}], n10=[{count=0, name=n10}]}]",results.toString());

		p.setGroupBy("count");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{0=[{count=0, name=n10}], 1=[{count=1, name=n9}], 2=[{count=2, name=n8}], 3=[{count=3, name=n7}], 4=[{count=4, name=n6}], 5=[{count=5, name=n5}], 6=[{count=6, name=n4}], 7=[{count=7, name=n3}], 8=[{count=8, name=n2}], 9=[{count=9, name=n1}]}]",results.toString());
	
		p.setGroupBy("name,count");
		p.open(null);
		results = p.process(rows);
		System.out.println(results);
		assertEquals("[{[n4, 6]=[{count=6, name=n4}], [n3, 7]=[{count=7, name=n3}], [n2, 8]=[{count=8, name=n2}], [n1, 9]=[{count=9, name=n1}], [n9, 1]=[{count=1, name=n9}], [n10, 0]=[{count=0, name=n10}], [n8, 2]=[{count=2, name=n8}], [n7, 3]=[{count=3, name=n7}], [n6, 4]=[{count=4, name=n6}], [n5, 5]=[{count=5, name=n5}]}]",results.toString());

	}
	
	public List<Map> newList(int count) {
		List r = new ArrayList();
		for(int i = 0; i < count; i++) {
			r.add(MapUtil.newMap("count",i));
		}
		return r;
	}

	public List<Map> newListWithName(int count) {
		List r = new ArrayList();
		for(int i = 0; i < count; i++) {
			r.add(MapUtil.newMap("count",i,"name","n"+(count-i)));
		}
		return r;
	}
	
}
