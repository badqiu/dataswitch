package com.duowan.dataswitch.output;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.duowan.dataswitch.TestUtil;

public class ScriptOutputTest {

	ScriptOutput output = new ScriptOutput();
	@Test
	public void test() {
		List<Object> rows = TestUtil.newTestDatas(20);
		output.setLang("groovy");
		output.setBeforeScript("System.out.println('before'); this.beforeVar = 'beforeValue';");
		output.setScript("System.out.println(username+' pwd:' + pwd + beforeVar);");
		output.setAfterScript("System.out.println('after execute')");
		output.write(rows);
		output.close();
	}

}
