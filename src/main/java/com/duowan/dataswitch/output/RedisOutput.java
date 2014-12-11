package com.duowan.dataswitch.output;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.mvel2.MVEL;
import org.springframework.util.Assert;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.duowan.common.redis.RedisTemplate;
import com.duowan.common.redis.RedisTransactionCallback;

public class RedisOutput implements Output{

	private JedisPool jedisPool;
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

	@Override
	public void write(List<Object> rows) {
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
	
	public static void batchRedis(JedisPool jedisPool,final List<Object> datas,final Map globalContext, final String script) throws Exception {
		Assert.hasText(script,"script must be not empty");
		Assert.notNull(jedisPool,"jedisPool must be not null");
		RedisTemplate template = new RedisTemplate(jedisPool);
		String newExpr = String.format("foreach(row : datas) { var redis = redis; %s}",script);
		final Serializable expr = MVEL.compileExpression(newExpr);
		template.execute(new RedisTransactionCallback(){
			@Override
			public Object doInTransaction(Transaction redis) {
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
