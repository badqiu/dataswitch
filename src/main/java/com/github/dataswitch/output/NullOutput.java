package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.BaseObject;
/**
 * 不做任何事情的Output
 * @author badqiu
 *
 */
public class NullOutput extends BaseObject implements Output{

	public void write(List<Map<String, Object>> rows) {
	}

}
