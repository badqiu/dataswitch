package com.github.dataswitch.util;
import java.util.ArrayList;

public class EconomicLeadingIndicator {

    public static double calculate(ArrayList<Double> gdp, ArrayList<Double> population, ArrayList<Double> consumerIndex) {
        // 计算经济领先指标
        // 假设gdp, population和consumerIndex包含历史的数据，以ArrayList<Double>的形式给出
        // 我们需要计算并返回未来三个月的经济领先指标

        //首先计算各指标的增长率
        double gdpGrowthRate = calculateGrowthRate(gdp);
        double populationGrowthRate = calculateGrowthRate(population);
        double consumerIndexGrowthRate = calculateGrowthRate(consumerIndex);

        // 根据公式计算经济领先指标
        double leadingIndicator = (0.2 * gdpGrowthRate) + (0.3 * populationGrowthRate) + (0.5 * consumerIndexGrowthRate);

        return leadingIndicator;
    }

    public static double calculateGrowthRate(ArrayList<Double> data) {
        // 计算增长率
        // 这里传入一个包含历史数据的ArrayList<Double>

        double current = data.get(data.size() - 1);
        double previous = data.get(data.size() - 2);

        double growthRate = (current - previous) / previous;
        return growthRate;
    }
}