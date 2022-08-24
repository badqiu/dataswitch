package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.rapid.common.util.FastBeanUtil;


public class ScriptOutput extends BaseObject implements Output{

	private String lang; //动态语言
	private String beforeScript; // script执行之前的脚本
	private String script; // write()时执行的脚本
	private String afterScript; // script执行之后的脚本
	
	private boolean rowEval = true; //是否每一行数据单独eval,不然将会传递rows参数,以作为数据引用
	
	private transient ScriptEngine engine;
	private Map context;
	private Output output;
	
	public ScriptOutput() {
	}
	
	public ScriptOutput(Output output) {
		this.output = output;
	}
	
	public Output getOutput() {
		return output;
	}

	public void setOutput(Output output) {
		this.output = output;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
	
	public String getBeforeScript() {
		return beforeScript;
	}

	public void setBeforeScript(String beforeScript) {
		this.beforeScript = beforeScript;
	}

	public String getAfterScript() {
		return afterScript;
	}

	public void setAfterScript(String afterScript) {
		this.afterScript = afterScript;
	}
	
	public Map getContext() {
		return context;
	}

	public void setContext(Map context) {
		this.context = context;
	}

	public boolean isRowEval() {
		return rowEval;
	}

	public void setRowEval(boolean perRowEval) {
		this.rowEval = perRowEval;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output.super.open(params);
		init();
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		try {
			
			if(rowEval) { //还可以支持每三种,script生成for循环,再eval
				for(Object row : rows) {
					javax.script.Bindings bindings = createBinding();
					if(row instanceof Map) {
						bindings.putAll((Map)row);
					}else {
						bindings.putAll(FastBeanUtil.describe(row)); //TODO,是否需要describe?
					}
					engine.eval(script, bindings); //script 性能优化,需要compiled
				}
			}else {
				javax.script.Bindings bindings = createBinding();
				bindings.put("rows", rows);
				engine.eval(script, bindings); //script 性能优化,需要compiled
			}
		}catch(Exception e) {
			throw new RuntimeException("eval error,id:"+getId()+" script:"+script,e);
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
		if(output != null) {
			bindings.put("output", output);
		}
		return bindings;
	}

	private Bindings beforeBinding;
	public void init() throws ScriptException {
		Assert.hasText(script,"script must be not empty");
		Assert.hasText(lang,"'lang' must be not null");
		
		ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
		engine = scriptEngineMgr.getEngineByName(lang);
		beforeBinding = evalIfNotBlank(engine,beforeScript);
	}

	public static Bindings evalIfNotBlank(ScriptEngine engine,String script) {
		try {
			if(StringUtils.isNotBlank(script)) {
				javax.script.Bindings bindings = engine.createBindings();
				engine.eval(script,bindings);
				return bindings;
			}
			return null;
		}catch(ScriptException e) {
			throw new RuntimeException("eval error,script:"+script,e);
		}
	}

	@Override
	public void close() {
		try {
			evalIfNotBlank(engine,afterScript);
		}catch(Exception e) {
			throw new RuntimeException("eval error,id:"+getId()+" script:"+afterScript,e);
		}
	}

	
}
