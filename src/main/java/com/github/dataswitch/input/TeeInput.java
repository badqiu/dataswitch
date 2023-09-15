package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;

import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.util.IOUtil;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 将 Input读进来的数据,透明的写入branch Output
 * 
 * @author badqiu
 *
 */
public class TeeInput extends ProxyInput{

	private Output[] branchs;
	
	 /**
     * Flag for closing also the associated output stream when this
     * stream is closed.
     */
    private boolean closeBranch;
    
    private FailMode failMode = FailMode.FAIL_FAST;
    
    public TeeInput() {
    }
    
    public TeeInput(Input input,Output branch) {
    	this(input,false,branch);
    }
    
	public TeeInput(Input proxy,boolean closeBranch,Output... branchs) {
		super(proxy);
		this.branchs = branchs;
		this.closeBranch = closeBranch;
	}
	
	public Output[] getBranchs() {
		return branchs;
	}

	public void setBranchs(Output... branchs) {
		this.branchs = branchs;
	}

	public void setBranch(Output branch) {
		setBranchs(branch);
	}
	
	public FailMode getFailMode() {
		return failMode;
	}

	public void setFailMode(FailMode failMode) {
		this.failMode = failMode;
	}

	public boolean isCloseBranch() {
		return closeBranch;
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		InputOutputUtil.openAll(failMode, params, branchs);
	}

	@Override
	public List<Object> read(int size) {
		List<Object> result =  super.read(size);
		
		failMode.forEach(branchs, branch -> {
			branch.write(result);
		});
		
		return result;
	}
	
	@Override
	public void close() throws Exception {
		try {
			super.close();
		}finally {
			if(closeBranch) {
				closeBranchs();
			}
		}
	}

	private void closeBranchs() {
		InputOutputUtil.closeAll(failMode, branchs);
	}
}
