package com.github.dataswitch.runner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.BaseObject;

public class Beans {	
	private List<Object> beans = new ArrayList<Object>();
	
	private File _sourceFile;
	
//	private String _paramsFile; //参数文件
//	private String _paramsUrl; //参数URL
//	private String _paramsGenerator; //参数生成类及方法
//	private String _paramsScript; //参数生成script
//	private String _paramsLanguage; //参数生成language
//	private String _paramsSql; //参数从数据库生成
	
	public Map<String,Object> toBeansMap() {
		Map<String,Object> result = new HashMap<String,Object>();
		for(Object bean : beans) {
			if(bean instanceof BaseObject) {
				BaseObject bo = (BaseObject)bean;
				result.put(bo.getId(), bo);
			}
		}
		
		return result;
	}

	public File getSourceFile() {
		return _sourceFile;
	}

	public void setSourceFile(File sourceFile) {
		this._sourceFile = sourceFile;
	}
	
}
