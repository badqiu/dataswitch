package com.github.dataswitch.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Enabled;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.enums.DataRouting;
import com.github.dataswitch.support.ExecutorServiceProvider;
import com.github.dataswitch.util.InputOutputUtil;

/**
 * 将一个输入copy一份输出在branch上
 * 
 * @author badqiu
 *
 */
public class MultiOutput extends BaseObject  implements Output{

	
	private static Logger logger = LoggerFactory.getLogger(TeeOutput.class);
	
	private Output[] branchs;
	private FailMode failMode = FailMode.FAIL_FAST;
	
	private boolean concurrent = false; //并发写
	
	private ExecutorServiceProvider executorService = new ExecutorServiceProvider();
	
	private DataRouting dataRouting = DataRouting.ALL;
	private long _sequence = 0;
	
	public MultiOutput() {
	}
	
	public MultiOutput(Output... branchs) {
		this.branchs = branchs;
	}
	
	public FailMode getFailMode() {
		return failMode;
	}

	public void setFailMode(FailMode failMode) {
		this.failMode = failMode;
	}
	
	public Output[] getBranchs() {
		return branchs;
	}

	public void setBranchs(Output... branchs) {
		this.branchs = branchs;
	}
	
	public DataRouting getDataRouting() {
		return dataRouting;
	}

	public void setDataRouting(DataRouting dataRouting) {
		this.dataRouting = dataRouting;
	}

	public boolean isConcurrent() {
		return concurrent;
	}

	public void setConcurrent(boolean concurrent) {
		this.concurrent = concurrent;
	}

	public ExecutorService getExecutorService() {
		return executorService.getExecutorService();
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService.setExecutorService(executorService);
	}

	public int getThreadPoolSize() {
		return executorService.getThreadPoolSize();
	}

	public void setThreadPoolSize(int threadPoolSize) {
		executorService.setThreadPoolSize(threadPoolSize);
	}

	public String getExecutorGroup() {
		return executorService.getExecutorGroup();
	}

	public void setExecutorGroup(String executorGroup) {
		executorService.setExecutorGroup(executorGroup);
	}

	public String getExecutorName() {
		return executorService.getExecutorName();
	}

	public void setExecutorName(String executorName) {
		executorService.setExecutorName(executorName);
	}

	@Override
	public void write(List<Object> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		if(ArrayUtils.isEmpty(branchs)) return;
		
		if(DataRouting.ALL == dataRouting) {
			failMode.forEach(branchs,(branch) -> {
				outputWrite(rows, branch);
			});
		}else {
			int index = getOutputIndexByLoadBalance();
			Output branch = branchs[index];
			outputWrite(rows,branch);
		}
	}

	private int getOutputIndexByLoadBalance() {
		if(dataRouting == DataRouting.RANDOM) {
			return RandomUtils.nextInt(branchs.length);
		} else if(dataRouting == DataRouting.ROUND_ROBIN) {
			return (int)(_sequence++ % branchs.length);
//		} else if(dataRouting == DataRouting.HASH) {
//			throw new RuntimeException("unsupported dataRouting:"+dataRouting);
		}
		
		
		throw new RuntimeException("unsupported dataRouting:"+dataRouting);
	}

	protected void outputWrite(List<Object> rows, Output branch) {
		if(concurrent) {
			getExecutorService().submit(() -> {
				branch.write(rows);
			});
		}else {
			branch.write(rows);
		}
	}

	@Override
	public void close() throws InterruptedException {
		InputOutputUtil.close(executorService);
		InputOutputUtil.closeAll(branchs);
	}
	
	@Override
	public void flush() throws IOException {
		InputOutputUtil.flushAll(branchs);
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		branchs = Enabled.filterByEnabled(branchs);
		InputOutputUtil.openAll(params,branchs);
		
		if(concurrent) {
			InputOutputUtil.open(params, executorService);
		}
	}
}
