package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.github.dataswitch.output.ScriptOutput;

public class ScriptProcessor implements Processor {

	private String lang; //动态语言
	private String beforeScript; // script执行之前的脚本
	private String script; // write()时执行的脚本
	
	private boolean rowEval = true; //是否每一行数据单独eval,不然将会传递rows参数,以作为数据引用
	
	private transient ScriptEngine engine;
	private Map context;
	private boolean inited = false;
	private Bindings beforeBinding;
	
	@Override
	public List<Object> process(List<Object> datas) throws Exception {
		synchronized(this) {
			if(!inited) {
				inited = true;
				ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
				engine = scriptEngineMgr.getEngineByName(lang);
				beforeBinding = ScriptOutput.evalIfNotBlank(engine, beforeScript);
			}
		}
		
		if(rowEval) {
			List<Object> resultList = new ArrayList<Object>();
			for(Object row : datas) {
				javax.script.Bindings bindings = createBinding();
				bindings.put("row", row);
				Object result = engine.eval(script, bindings); //TODO script 性能优化,需要compiled
				if(result != null) {
					resultList.add(result);
				}
			}
			return resultList;
		}else {
			javax.script.Bindings bindings = createBinding();
			bindings.put("rows", datas);
			return (List<Object>)engine.eval(script, bindings); //TODO script 性能优化,需要compiled
		}
	}
	
	private javax.script.Bindings createBinding() {
		javax.script.Bindings bindings = engine.createBindings();
		if(context != null) {
			bindings.putAll(context);
		}
		if(beforeBinding != null) {
			bindings.putAll(beforeBinding);
		}
		return bindings;
	}

}
