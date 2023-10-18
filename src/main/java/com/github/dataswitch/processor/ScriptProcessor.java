package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.github.dataswitch.output.ScriptOutput;

/**
 * 
 * @author badqiu
 *
 */
public class ScriptProcessor implements Processor {

	private String language; // 动态语言
	private String initScript; // script执行之前的脚本,可以执行初始化操作
	private String script; // write()时执行的脚本

	private boolean rowEval = true; // 是否每一行数据单独eval,不然将会传递rows参数,以作为数据引用
	private Map context; /* 脚本上下文 */

	private transient ScriptEngine scriptEngine;
	private Bindings initBinding;
	private CompiledScript compiledScript;
	
	public ScriptProcessor() {
	}

	public ScriptProcessor(String language, String script) {
		super();
		this.language = language;
		this.script = script;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String lang) {
		this.language = lang;
	}
	
	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public ScriptEngine getScriptEngine() {
		return scriptEngine;
	}

	public void setScriptEngine(ScriptEngine scriptEngine) {
		this.scriptEngine = scriptEngine;
	}

	public String getInitScript() {
		return initScript;
	}

	public void setInitScript(String initScript) {
		this.initScript = initScript;
	}

	public boolean isRowEval() {
		return rowEval;
	}

	public void setRowEval(boolean rowEval) {
		this.rowEval = rowEval;
	}

	public Map getContext() {
		return context;
	}

	public void setContext(Map context) {
		this.context = context;
	}

	@Override
	public List<Map<String, Object>> process(List<Map<String, Object>> datas) throws Exception {
		if(StringUtils.isBlank(script)) {
			return datas;
		}
		

		if (rowEval) {
			List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
			for (Object row : datas) {
				javax.script.Bindings bindings = createBinding();
				bindings.put("row", row);
				Map<String,Object> result = eval(bindings);
				if (result != null) {
					resultList.add(result);
				}
			}
			return resultList;
		} else {
			javax.script.Bindings bindings = createBinding();
			bindings.put("rows", datas);
			return (List<Map<String, Object>>)eval(bindings); 
		}
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Processor.super.open(params);
		init(params);
	}

	private synchronized void init(Map<String, Object> params) throws ScriptException {
		lookupScriptEngine();
		initBinding = ScriptOutput.evalIfNotBlank(scriptEngine, initScript,params);
		
		if(scriptEngine instanceof Compilable) {
			Compilable compilable = (Compilable)scriptEngine;
			compiledScript = compilable.compile(script);
		}
	}

	private Map eval(javax.script.Bindings bindings)throws ScriptException {
		if(compiledScript == null) {
			return (Map)scriptEngine.eval(script, bindings);
		}else {
			return (Map)compiledScript.eval(bindings);
		}
	}

	private void lookupScriptEngine() {
		if(scriptEngine == null) {
			ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
			Assert.hasText(language,"language must be not empty for lookup ScriptEngine");
			scriptEngine = scriptEngineMgr.getEngineByName(language);
		}
	}

	private javax.script.Bindings createBinding() {
		javax.script.Bindings bindings = scriptEngine.createBindings();
		if (context != null) {
			bindings.putAll(context);
		}
		if (initBinding != null) {
			bindings.putAll(initBinding);
		}
		return bindings;
	}

}
