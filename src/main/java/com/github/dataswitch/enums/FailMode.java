package com.github.dataswitch.enums;

import org.apache.commons.lang.StringUtils;

public enum FailMode {
	FAIL_FAST("failFast","快速失败"),
	FAIL_AT_END("failAtEnd","结束时失败"),
	FAIL_NEVER("failNever","从不失败");
	

	private final String shortName;
	private final String desc;
	
	FailMode(String shortName,String desc) {
		this.shortName = shortName;
		this.desc = desc;
	}

	public String getShortName() {
		return shortName;
	}
	
	public String getDesc() {
		return desc;
	}

	public static FailMode getRequiredByName(String name) {
		if(StringUtils.isBlank(name)) {
			return null;
		}
		
		name = StringUtils.trim(name);
		
		for(FailMode m : values()) {
			if(m.getShortName().equalsIgnoreCase(name)) {
				return m;
			}
			if(m.name().equalsIgnoreCase(name)) {
				return m;
			}
		}
		throw new RuntimeException("not found failMode by name:"+name);
	}
}
