package com.github.dataswitch.output;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.enums.OutputMode;
import com.github.dataswitch.util.MapUtil;
import com.mongodb.client.FindIterable;

public class MongodbOutputTest {

	public static String mongodbUrl = "mongodb://172.17.38.121:27017/?directConnection=true&serverSelectionTimeoutMS=2000";
	MongodbOutput output = new MongodbOutput();
	int dataCount = 5;
	@Before
	public void before() throws Exception {
		output.setUrl(mongodbUrl);
		output.setDatabase("test");
		output.setCollection("badqiu_test");
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
	
	@Test
	public void testWrite() throws Exception {
		writeByOutputMode(OutputMode.insert);
		assertEquals(0,writeByOutputMode(OutputMode.delete));
		
		output._mongoCollection.deleteMany(new Document());
		assertEquals(dataCount,writeByOutputMode(OutputMode.insert));
		
		output._mongoCollection.deleteMany(new Document());
		assertEquals(dataCount,writeByOutputMode(OutputMode.upsert));
		
		output._mongoCollection.deleteMany(new Document());
		assertEquals(0,writeByOutputMode(OutputMode.update));
	}

	private int writeByOutputMode(OutputMode outputMode) throws Exception {
		System.out.println("============= "+ outputMode);
		output.setPrimaryKeys("name");
		output.setOutputMode(outputMode);
		output.open(null);
		
		List rows = new ArrayList<Map>();
		for(int i = 0; i < dataCount; i++) {
			long now = System.currentTimeMillis();
			Map row = MapUtil.newMap("name","badqiu-"+i,"age",20,"count",i,"birthDate",new java.sql.Date(now));
			rows.add(row);
		}
		output.write(rows);
		
		FindIterable<Document> find = output._mongoCollection.find();
		return print(find);
	}

	private int print(FindIterable<Document> find) {
		final AtomicInteger count = new AtomicInteger();
		find.forEach((item) -> {
			System.out.println("item:"+item);
			count.incrementAndGet();
		});
		return count.intValue();
	}

}
