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

	private Output branch;
	
	public TeeOutput(Output out,Output branch) {
		super(out);
		this.branch = branch;
	}
	
	@Override
	public synchronized void write(List<Object> rows) {
		super.write(rows);
		this.branch.write(rows);
	}

	@Override
	public void close() {
		super.close();
		IOUtil.closeQuietly(branch);
	}

}
