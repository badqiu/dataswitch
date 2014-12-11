package com.duowan.dataswitch.output;

import java.util.List;

import com.duowan.dataswitch.util.IOUtil;
/**
 * 将一个输入copy一份输出在branch上
 * 
 * @author badqiu
 *
 */
public class TeeOutput extends ProxyOutput implements Output{

	private Output[] branchs;
	
	public TeeOutput(Output out,Output... branchs) {
		super(out);
		this.branchs = branchs;
	}
	
	@Override
	public synchronized void write(List<Object> rows) {
		super.write(rows);
		for(Output branch : branchs)
			branch.write(rows);
	}

	@Override
	public void close() {
		super.close();
		for(Output branch : branchs)
			IOUtil.closeQuietly(branch);
	}

}
