package com.github.dataswitch.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.github.dataswitch.util.MapUtil;

public class ScriptProcessorTest {

	@Before
	public void before() {
		System.out.println("--------------------------------------------------\n");
	}
	
	@Test
	public void testRowEval() throws Exception {
		 ScriptProcessor sp = newScriptProcessor();
		 sp.setScript("println 'row eval,name:'+name+' row='+row; return row;");
		 
		 List rows = new ArrayList();
		 rows.add("1");
		 rows.add("2");
		 Object result = sp.process(rows);
		 System.out.println("result:"+result);
		 assertEquals("[1, 2]",result.toString());
	}
	
	@Test
	public void testRowEvalFalse() throws Exception {
		 ScriptProcessor sp = newScriptProcessor();
		 sp.setRowEval(false);
		 sp.setScript("println 'row eval,name:'+name+' rows='+rows");
		 
		 List rows = new ArrayList();
		 rows.add("1");
		 rows.add("2");
		 Object result = sp.process(rows);
		 System.out.println("result:"+result);
		 assertNull(result);
	}

	private ScriptProcessor newScriptProcessor() {
		ScriptProcessor sp = new ScriptProcessor();
		sp.setLang("groovy");
		sp.setRowEval(true);
		sp.setInitScript("println 'initScript executed'");

		sp.setContext(MapUtil.newMap("name", "badqiu"));
		return sp;
	}

}
