package com.github.dataswitch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

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
	private boolean inited = false;
	private Bindings initBinding;

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
		synchronized (this) {
			if (!inited) {
				inited = true;
				lookupScriptEngine();
				initBinding = ScriptOutput.evalIfNotBlank(scriptEngine, initScript);
			}
		}

		if (rowEval) {
			List<Object> resultList = new ArrayList<Object>();
			for (Object row : datas) {
				javax.script.Bindings bindings = createBinding();
				bindings.put("row", row);
				Object result = scriptEngine.eval(script, bindings); // TODO script
																// 性能优化,需要compiled
				if (result != null) {
					resultList.add(result);
				}
			}
			return resultList;
		} else {
			javax.script.Bindings bindings = createBinding();
			bindings.put("rows", datas);
			return (List<Object>) scriptEngine.eval(script, bindings); // TODO script
																	// 性能优化,需要compiled
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
