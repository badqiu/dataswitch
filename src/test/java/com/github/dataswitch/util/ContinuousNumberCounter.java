package com.github.dataswitch.util;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.math.RandomUtils;

/**
 * 测试连续开大小的次数
 * 
 * @author badqiu
 *
 */
public class ContinuousNumberCounter {
	
	private Map<Integer,AtomicInteger> stat = new HashMap();

	public void exec() {
		long loopCount = 10000L * 10000;

    	int numberCount = 1; // 连续数字的数量
    	int maxNumberCount = 0; //最大的连续数字
        int previousNumber = 0; // 上一个输入的数字
        
		for(long i = 0; i < loopCount; i++) {
            int number = RandomUtils.nextInt(2);
           
            if (number == previousNumber) {
                numberCount++;
            } else {
            	statNumberCount(numberCount);
                numberCount = 1; // 重新开始计数
            }
            
            previousNumber = number;
            
            if(maxNumberCount < numberCount) {
            	maxNumberCount = numberCount;
            }
            
            
        }
        
        System.out.println("开大小，连续开大的数量为: " + maxNumberCount);
        
        System.out.println("玩"+(loopCount/10000)+"万次大小，连续N次开大的概率");
        
        DecimalFormat df = new DecimalFormat("#.####%");
        stat.forEach((num,count) -> {
        	double percent = (double)count.intValue() / loopCount / 2;
//			System.out.println(num+"\t"+count+"\t\t"+df.format(percent));
			System.out.println(num+"\t\t"+df.format(percent));
        });
	}

	private void statNumberCount(int numberCount) {
		AtomicInteger c = stat.get(numberCount);
		if(c == null) {
			stat.put(numberCount,new AtomicInteger());
			return;
		}
		
		c.incrementAndGet();
	}
	
    public static void main(String[] args) {
    	ContinuousNumberCounter counter = new ContinuousNumberCounter();
		counter.exec();
    }
    
}