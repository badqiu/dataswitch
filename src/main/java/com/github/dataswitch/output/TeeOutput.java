package com.github.dataswitch.output;

/**
 * 将一个输入copy一份输出在branch上
 * 
 * @author badqiu
 *
 */
public class TeeOutput extends MultiOutput{

	public TeeOutput() {
		super();
	}

	public TeeOutput(Output... branchs) {
		super(branchs);
	}
	
}
