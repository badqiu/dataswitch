package com.github.dataswitch.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.util.MapUtil;

public class HbaseOutputTest {

	public static String mongodbUrl = "mongodb://172.17.38.121:27017/?directConnection=true&serverSelectionTimeoutMS=2000";
	HbaseOutput output = new HbaseOutput();
	int dataCount = 5;
	public static String hbaseConfig = "hbase.zookeeper.quorum=172.17.38.121:2181";
	@Before
	public void before() throws Exception {
		output.setHbaseConfig(hbaseConfig);
		output.setTable("test_hbase_output");
		output.setCreateTable(true);
		output.setFamily("f");
		output.setRowkeyColumn("name");
		output.setVersionColumn("time");
		output.setSkipNull(false);
		output.setSkipWal(true);
	}
	
	@After
	public void after() throws Exception {
		output.close();
	}
	
	@Test
	public void testWrite_by_enums() throws Exception {
		for(OutputMode outputMode : OutputMode.values()) {
			writeByOutputMode(OutputMode.delete);
			writeByOutputMode(outputMode);
		}
	}
	


	private void writeByOutputMode(OutputMode outputMode) throws Exception {
		System.out.println("============= "+ outputMode);
		output.setOutputMode(outputMode);
		output.open(null);
		
		List rows = new ArrayList<Map>();
		for(int i = 0; i < dataCount; i++) {
			Map row = MapUtil.newMap("name","badqiu-"+i,"age",20,"count",i,"time",System.currentTimeMillis());
			rows.add(row);
		}
		output.write(rows);
		
		print(rows);
	}

	private int print(List<Object> find) {
		final AtomicInteger count = new AtomicInteger();
		find.forEach((item) -> {
			System.out.println("item:"+item);
			count.incrementAndGet();
		});
		return count.intValue();
	}

}
