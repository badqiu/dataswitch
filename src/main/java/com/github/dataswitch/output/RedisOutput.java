package com.github.dataswitch.output;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.Transaction;
import org.mvel2.MVEL;
import org.springframework.util.Assert;

import redis.clients.jedis.JedisPool;

import com.github.dataswitch.BaseObject;
import com.github.rapid.common.redis.RedisTemplate;
import com.github.rapid.common.redis.RedisTransactionCallback;

public class RedisOutput extends BaseObject implements Output{

	private JedisPool jedisPool;
	private String url;
	private String script;
//	private String beforeScript;
//	private String afterScript;
	private Map context;
	
	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
	
	public Map getContext() {
		return context;
	}

	public void setContext(Map context) {
		this.context = context;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public void write(List<Map<String, Object>> rows) {
		if(CollectionUtils.isEmpty(rows)) return;
		
		try {
			batchRedis(jedisPool, rows,context,script);
		} catch (Exception e) {
			throw new RuntimeException("redis error,script:"+script,e);
		}
	}

	@Override
	public void close() {
		jedisPool.destroy();
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		jedisPool = new JedisPool(url);
	}
	
	public static void batchRedis(JedisPool jedisPool,final List<Map<String, Object>> datas,final Map globalContext, final String script) throws Exception {
		Assert.hasText(script,"script must be not empty");
		Assert.notNull(jedisPool,"jedisPool must be not null");
		RedisTemplate template = new RedisTemplate(jedisPool);
		String newExpr = String.format("foreach(row : datas) { var redis = redis; %s}",script);
		final Serializable expr = MVEL.compileExpression(newExpr);
		template.execute(new RedisTransactionCallback(){
			@Override
			public Object doInTransaction(redis.clients.jedis.Transaction redis) {
				Map vars = new HashMap();
				if(globalContext != null) {
					vars.putAll(globalContext);
				}
				vars.put("datas", datas);
				vars.put("redis", redis);
				MVEL.executeExpression(expr,vars);
				redis.exec();
				return null;
			}
		});
	}
	
}
