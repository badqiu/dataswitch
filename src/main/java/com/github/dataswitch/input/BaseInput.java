package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.util.Util;

public abstract class BaseInput extends BaseObject implements Input{

	@Override
	public List<Object> read(int size) {
		return read(this,size);
	}
	
	public static List<Object> read(BaseInput input,int size) {
		List<Object> result = new ArrayList<Object>();
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
