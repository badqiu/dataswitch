package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.Util;

public abstract class BaseInput extends BaseObject implements Input{

	@Override
	public List<Map<String, Object>> read(int size) {
		return read(this,size);
	}
	
	public static List<Map<String, Object>> read(BaseInput input,int size) {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		for(int i = 0; i < size; i++) {
			Object obj = input.readObject();
			if(obj == null) {
				break;
			}
			
			Collection collection = Util.oneToList(obj);
			if(CollectionUtils.isEmpty(collection)) {
				break;
			}
			
			result.addAll(collection);
			
			if(result.size() > size) {
				break;
			}
		}
		return result;
	}
	

	public abstract Object readObject();

}
