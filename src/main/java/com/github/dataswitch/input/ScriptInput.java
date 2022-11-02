package com.github.dataswitch.input;

import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.springframework.util.Assert;

import com.github.dataswitch.output.ScriptOutput;

public class ScriptInput extends BaseInput{
	private String language; //动态语言
	private String beforeScript; // script执行之前的脚本
	private String script; // write()时执行的脚本
	private String afterScript; // script执行之后的脚本
	
	private transient ScriptEngine engine;
	private Map context;
	
	public String getLanguage() {
		return language;
	}

	public void setLanguage(String lang) {
		this.language = lang;
	}
	
	public String getBeforeScript() {
		return beforeScript;
	}

	public void setBeforeScript(String beforeScript) {
		this.beforeScript = beforeScript;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
	
	public Map getContext() {
		return context;
	}

	public void setContext(Map context) {
		this.context = context;
	}

	public String getAfterScript() {
		return afterScript;
	}

	public void setAfterScript(String afterScript) {
		this.afterScript = afterScript;
	}

	@Override
	public Object readObject() {
		try {
			javax.script.Bindings bindings = createBindings();
			return eval(bindings);
		}catch(ScriptException e) {
			throw new RuntimeException("eval error,script:"+script,e);
		}
	}

	private Object eval(Bindings bindings) throws ScriptException {
		if(bindings == null) {
			return engine.eval(script);
		}else {
			return engine.eval(script,bindings);
		}
	}

	private Bindings createBindings() {
		Bindings map = engine.createBindings();
		if(context != null) {
			map.putAll(context);
		}
		if(beforeBindings != null) {
			map.putAll(beforeBindings);
		}
		return map;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		init(params);
	}

	private Bindings beforeBindings;
	private void init(Map<String, Object> params) {
		ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
		Assert.hasText(language,"'language' must be not null");
		engine = scriptEngineMgr.getEngineByName(language);
		
		Assert.hasText(script,"script must be not empty");
		beforeBindings = ScriptOutput.evalIfNotBlank(engine,beforeScript,params);
	}

	
	public void close() {
		ScriptOutput.evalIfNotBlank(engine,afterScript,null);
	}

}
