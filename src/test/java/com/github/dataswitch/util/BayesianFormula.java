package com.github.dataswitch.util;

/**
 *  贝叶斯公式 示例
 *  
 * @author badqiu
 *
 */
public class BayesianFormula {
    
    /**
     * 计算后验概率
     * @param prior 先验概率概率，即我们对某个假设在有证据之前的初始信心水平
     * @param likelihood 似然率，即给定证据时，假设成立的可能性
     * @param evidence 证据概率，出现某个证据的概率，也叫作边缘似然率
     * @return 后验概率，即根据新证据更新后的假设概率
     */
    public static double calculatePosteriorProbability(double a_prior, double a_likelihood, double b_evidence) {
        // 根据贝叶斯公式计算后验概率
        double posterior = (a_likelihood * a_prior) / b_evidence;
        return posterior;
    }
 
    public static void main(String[] args) {
        // 假设我们需要测试的条件：A、B、C 三个事件，均有一定的概率发生
        // 我们最初对事件 A 发生的先验概率并不高，变量 prior 取值为 0.01
        double prior = 0.01;
        
        // 似然率 likelihood 是指，在出现某个证据时，A 事件成立的可能性
        // 本例中，我们假设 A 事件发生与事件 B 和 C 的发生有关，likelihood 取值为 0.9
        double likelihood = 0.9;
        
        // 证据 evidence 是指一组证据或发生的事件的概率，我们将事件 B 和 C 的概率相加
        // 本例中，我们假设事件 B 和事件 C 发生的总概率为 0.11
        double evidence = 0.11;
 
        // 计算更新后的后验概率
        double posterior = calculatePosteriorProbability(prior, likelihood, evidence);
        
        // 输出计算结果
        double percent = (long)(posterior * 10000) / 100.0;
		System.out.println("事件 A 发生的后验概率为：" + percent + "%");
    }
}