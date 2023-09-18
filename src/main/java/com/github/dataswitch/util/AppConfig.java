package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


/**
 * 用于获取默认App配置的类
 * @author badqiu
 *
 */
public class AppConfig {
	
	private Map config = new HashMap();
	
	public AppConfig() {
	}
	
	public AppConfig(Map config) {
		this.config = config;
	}

	public String getProjectNamespace() {
		return getProperty("PROJECT_NAMESPACE","DEFAULT_PROJECT_NAMESPACE");
	}
	
	public String getProjectCode() {
		return getProperty("PROJECT_CODE","DEFAULT_PROJECT_CODE");
	}
	
	public String getProjectName() {
		return getProperty("PROJECT_NAME","DEFAULT_PROJECT_NAME");
	}
	
	public String getRunUser() {
		return getProperty("RUN_USER","DEFAULT_RUN_USER");
	}
	
	public String getProjectNamespaceWithCodePath() {
		return "/" + getProjectNamespace()+"/"+getProjectCode()+"/";
	}
	
	public String getProjectAppPath() {
		String path = getProperty("PROJECT_APP_PATH","/usr/local/") + getProjectNamespaceWithCodePath();
		return path;
	}
	
	public String getProjectLogPath() {
		String path = getProperty("PROJECT_LOG_PATH","/data/logs/") + getProjectNamespaceWithCodePath();;
		return path;
	}
	
	public String getProjectDataPath() {
		String path = getProperty("PROJECT_DATA_PATH","/data/data/") + getProjectNamespaceWithCodePath();;
		return path;
	}
	
	public String getProjectTmpPath() {
		String path = getProperty("java.io.tmpdir") + getProjectNamespaceWithCodePath();
		return path;
	}
	
	public String getProjectUserHome() {
		String path = getProperty("user.home") + getProjectNamespaceWithCodePath();;
		return path;
	}
	
	private String getProperty(String key) {
		return getProperty(key,null);
	}
	
	private String getProperty(String key,String defaultValue) {
		String value = (String)config.get(key);
		if(StringUtils.isBlank(value)) {
			value = System.getProperty(key);
		}
		if(StringUtils.isBlank(value)) {
			value = System.getenv(key);
		}
		
		if(StringUtils.isBlank(value)) {
			value = defaultValue;
		}
		
		return value;
	}
	
	public String getRequiredProerpty(String key) {
		String value = getProperty(key);
		
		if(StringUtils.isBlank(value)) {
			throw new IllegalStateException("not found config or system property or env by key:"+key);
		}
		return value;
	}

}
