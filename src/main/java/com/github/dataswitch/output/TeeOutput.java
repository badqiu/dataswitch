package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Enabled;
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
		this.failMode = failMode;
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		failMode.forEach(branchs,(branch) -> {
			branch.write(rows);
		});
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
		branchs = Enabled.filterByEnabled(branchs);
		InputOutputUtil.openAll(params,branchs);
	}
	
}
