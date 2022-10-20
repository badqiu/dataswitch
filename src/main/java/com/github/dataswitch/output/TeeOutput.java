package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.util.IOUtil;
import com.github.dataswitch.util.InputOutputUtil;
/**
 * 将一个输入copy一份输出在branch上
 * 
 * @author badqiu
 *
 */
public class TeeOutput extends BaseObject  implements Output{
	private static Logger logger = LoggerFactory.getLogger(TeeOutput.class);
	private Output[] branchs;
	private FailMode failMode = FailMode.FAIL_FAST;
	
	public TeeOutput() {
	}
	
	public TeeOutput(Output... branchs) {
		this.branchs = branchs;
	}
	
	public FailMode getFailMode() {
		return failMode;
	}
	
	public Output[] getBranchs() {
		return branchs;
	}

	public void setBranchs(Output... branchs) {
		this.branchs = branchs;
	}

	public void setFailMode(FailMode failMode) {
		if(failMode == FailMode.FAIL_FAST || failMode == FailMode.FAIL_NEVER) {
			this.failMode = failMode;
		}else {
			throw new IllegalArgumentException("not supported failMode:" + failMode);
		}
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		for(Output branch : branchs) {
			try {
				branch.write(rows);
			}catch(Exception e) {
				handleWriteException(branch, e);
			}
		}
	}

	protected void handleWriteException(Output branch, Exception e) {
		if(FailMode.FAIL_FAST == failMode) {
			throw new RuntimeException("error output on:"+branch,e);
		}else if(FailMode.FAIL_NEVER == failMode) {
			//ignore
			logger.error("error output on:"+branch,e);
		}else {
			throw new RuntimeException("error failMode:"+failMode);
		}
	}

	@Override
	public void close() {
		InputOutputUtil.closeAllQuietly(branchs);
	}
	
	@Override
	public void flush() throws IOException {
		InputOutputUtil.flushAll(branchs);
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		InputOutputUtil.openAll(params,branchs);
	}
	
}
