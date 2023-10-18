package com.github.dataswitch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParamsGenerator {

	public static class ParamList<T> {
		private String varName;
		private List<T> params;
	}
	
	public void forEach(List<ParamList> list) {
		List<Map> results = new ArrayList();
		for(ParamList paramList : list) {
			for(Object param : paramList.params) {
				Map map = new HashMap();
				map.put(paramList.varName,param);
				results.add(map);
			}
		}
	}
}
