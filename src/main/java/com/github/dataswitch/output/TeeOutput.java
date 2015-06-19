package com.github.dataswitch.output;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.github.dataswitch.util.IOUtil;
/**
 * 将一个输入copy一份输出在branch上
 * 
 * @author badqiu
 *
 */
public class TeeOutput  implements Output{

	private Output[] branchs;
	
	public TeeOutput(Output... branchs) {
		this.branchs = branchs;
	}
	
	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		for(Output branch : branchs)
			branch.write(rows);
	}

	@Override
	public void close() {
		for(Output branch : branchs)
			IOUtil.closeQuietly(branch);
	}

}
