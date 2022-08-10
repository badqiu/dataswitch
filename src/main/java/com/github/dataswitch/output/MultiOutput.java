package com.github.dataswitch.output;

public class MultiOutput extends TeeOutput{

	public MultiOutput(Output... branchs) {
		super(branchs);
	}
	
}
