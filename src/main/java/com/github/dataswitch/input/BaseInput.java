package com.github.dataswitch.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
			result.addAll(collection);
		}
		return result;
	}
	

	public abstract Object readObject();

}
