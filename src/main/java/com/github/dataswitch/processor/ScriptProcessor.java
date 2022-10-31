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

	private String lang; // 动态语言
	private String initScript; // script执行之前的脚本,可以执行初始化操作
	private String script; // write()时执行的脚本

	private boolean rowEval = true; // 是否每一行数据单独eval,不然将会传递rows参数,以作为数据引用
	private Map context; /* 脚本上下文 */

	private transient ScriptEngine scriptEngine;
	private Bindings initBinding;
	private CompiledScript compiledScript;
	
	public ScriptProcessor() {
	}

	public ScriptProcessor(String lang, String script) {
		super();
		this.lang = lang;
		this.script = script;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}
	
	public void setLanguage(String lang) {
		setLang(lang);
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
	public List<Object> process(List<Object> datas) throws Exception {
		if(StringUtils.isBlank(script)) {
			return datas;
		}
		

		if (rowEval) {
			List<Object> resultList = new ArrayList<Object>();
			for (Object row : datas) {
				javax.script.Bindings bindings = createBinding();
				bindings.put("row", row);
				Object result = eval(bindings);
				if (result != null) {
					resultList.add(result);
				}
			}
			return resultList;
		} else {
			javax.script.Bindings bindings = createBinding();
			bindings.put("rows", datas);
			return (List<Object>)eval(bindings); 
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

	private Object eval(javax.script.Bindings bindings)throws ScriptException {
		if(compiledScript == null) {
			return scriptEngine.eval(script, bindings);
		}else {
			return compiledScript.eval(bindings);
		}
	}

	private void lookupScriptEngine() {
		if(scriptEngine == null) {
			ScriptEngineManager scriptEngineMgr = new ScriptEngineManager();
			Assert.hasText(lang,"lang must be not empty for lookup ScriptEngine");
			scriptEngine = scriptEngineMgr.getEngineByName(lang);
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
