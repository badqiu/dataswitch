package com.duowan.dataswitch.input;

import java.util.List;

import com.duowan.dataswitch.output.Output;
import com.duowan.dataswitch.util.IOUtil;

/**
 * 将 Input读进来的数据,透明的写入branch Output
 * 
 * @author badqiu
 *
 */
public class TeeInput extends ProxyInput{

	private Output branch;
	 /**
     * Flag for closing also the associated output stream when this
     * stream is closed.
     */
    private final boolean closeBranch;
    
    public TeeInput(Input input,Output branch) {
    	this(input,branch,false);
    }
    
	public TeeInput(Input proxy,Output branch,boolean closeBranch) {
		super(proxy);
		this.branch = branch;
		this.closeBranch = closeBranch;
	}

	@Override
	public List<Object> read(int size) {
		List<Object> result =  super.read(size);
		branch.write(result);
		return result;
	}
	
	@Override
	public void close() {
		try {
			super.close();
		}finally {
			if(closeBranch) {
				IOUtil.closeQuietly(branch);
			}
		}
	}
}
