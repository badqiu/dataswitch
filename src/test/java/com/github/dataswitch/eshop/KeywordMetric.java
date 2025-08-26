package com.github.dataswitch.eshop;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 关键词搜索指标数据实体
 */
public class KeywordMetric {

    public static void main(String[] args) throws IOException {
        File jsonFile = ResourceUtils.getFile("classpath:test_keyword_list.json");
        String jsonString = FileUtils.readFileToString(jsonFile, "UTF-8");
        
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonString);
            
            // 提取data数组中的第一个元素
            JsonNode dataNode = rootNode.path("data")
                    .path("module_data")
                    .path("info_list")
                    .path("compass_general_table_value")
                    .path("data")
                    .get(0)
                    .path("cell_info");
            
            // 将JSON节点转换为KeywordMetric对象
            KeywordMetric metric = objectMapper.treeToValue(dataNode, KeywordMetric.class);
            
            // 输出解析结果
            System.out.println("搜索词: " + metric.getQuery().getValue().getValue_str());
            System.out.println("排名: " + metric.getRank().getValue().getValue());
            System.out.println("竞争指数: " + metric.getCompete_index().getIndex_values().getValue().getValue());
            
            // 输出热门商品信息
            if (metric.getHot_product() != null) {
                System.out.println("热门商品:");
                for (KeywordMetric.Product product : metric.getHot_product().getProduct_list()) {
                    System.out.println("  - " + product.getProduct_name());
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 搜索词信息
     */
    private IndexValue query;

    /**
     * 搜索结果曝光人数
     */
    private IndexValue search_show_ucnt;

    /**
     * 搜索人数
     */
    private IndexValue search_ucnt;

    /**
     * 搜索用户支付金额
     */
    private IndexValue pay_amt;

    /**
     * 商品曝光人数
     */
    private IndexValue product_show_ucnt;

    /**
     * 商品点击率(人数)
     */
    private IndexValue prod_show_click_ratio;

    /**
     * 商品点击-成交转化率(人数)
     */
    private IndexValue prod_click_pay_ratio;

    /**
     * 搜索成交订单数
     */
    private IndexValue pay_cnt;

    /**
     * 搜索结果点击人数
     */
    private IndexValue search_click_ucnt;

    /**
     * 搜索成交人数
     */
    private IndexValue pay_ucnt;

    /**
     * 竞争指数
     */
    private IndexValue compete_index;

    /**
     * 搜索客单价
     */
    private IndexValue pay_per_usr_price;

    /**
     * 平台曝光商品数
     */
    private IndexValue show_product_cnt;

    /**
     * 词下热门商品
     */
    private HotProduct hot_product;

    /**
     * 词下本店曝光商品
     */
    private ShopShowProduct shop_show_product;

    /**
     * 排名
     */
    private IndexValue rank;

    // Getters and Setters
    public IndexValue getQuery() { return query; }
    public void setQuery(IndexValue query) { this.query = query; }

    public IndexValue getSearch_show_ucnt() { return search_show_ucnt; }
    public void setSearch_show_ucnt(IndexValue search_show_ucnt) { this.search_show_ucnt = search_show_ucnt; }

    public IndexValue getSearch_ucnt() { return search_ucnt; }
    public void setSearch_ucnt(IndexValue search_ucnt) { this.search_ucnt = search_ucnt; }

    public IndexValue getPay_amt() { return pay_amt; }
    public void setPay_amt(IndexValue pay_amt) { this.pay_amt = pay_amt; }

    public IndexValue getProduct_show_ucnt() { return product_show_ucnt; }
    public void setProduct_show_ucnt(IndexValue product_show_ucnt) { this.product_show_ucnt = product_show_ucnt; }

    public IndexValue getProd_show_click_ratio() { return prod_show_click_ratio; }
    public void setProd_show_click_ratio(IndexValue prod_show_click_ratio) { this.prod_show_click_ratio = prod_show_click_ratio; }

    public IndexValue getProd_click_pay_ratio() { return prod_click_pay_ratio; }
    public void setProd_click_pay_ratio(IndexValue prod_click_pay_ratio) { this.prod_click_pay_ratio = prod_click_pay_ratio; }

    public IndexValue getPay_cnt() { return pay_cnt; }
    public void setPay_cnt(IndexValue pay_cnt) { this.pay_cnt = pay_cnt; }

    public IndexValue getSearch_click_ucnt() { return search_click_ucnt; }
    public void setSearch_click_ucnt(IndexValue search_click_ucnt) { this.search_click_ucnt = search_click_ucnt; }

    public IndexValue getPay_ucnt() { return pay_ucnt; }
    public void setPay_ucnt(IndexValue pay_ucnt) { this.pay_ucnt = pay_ucnt; }

    public IndexValue getCompete_index() { return compete_index; }
    public void setCompete_index(IndexValue compete_index) { this.compete_index = compete_index; }

    public IndexValue getPay_per_usr_price() { return pay_per_usr_price; }
    public void setPay_per_usr_price(IndexValue pay_per_usr_price) { this.pay_per_usr_price = pay_per_usr_price; }

    public IndexValue getShow_product_cnt() { return show_product_cnt; }
    public void setShow_product_cnt(IndexValue show_product_cnt) { this.show_product_cnt = show_product_cnt; }

    public HotProduct getHot_product() { return hot_product; }
    public void setHot_product(HotProduct hot_product) { this.hot_product = hot_product; }

    public ShopShowProduct getShop_show_product() { return shop_show_product; }
    public void setShop_show_product(ShopShowProduct shop_show_product) { this.shop_show_product = shop_show_product; }

    public IndexValue getRank() { return rank; }
    public void setRank(IndexValue rank) { this.rank = rank; }

    /**
     * 指标值内部类
     */
    public static class IndexValue {
        private int cell_type;
        private IndexValues index_values;
        private Value value;

        // Getters and Setters
        public int getCell_type() { return cell_type; }
        public void setCell_type(int cell_type) { this.cell_type = cell_type; }

        public IndexValues getIndex_values() { return index_values; }
        public void setIndex_values(IndexValues index_values) { this.index_values = index_values; }
        
        public Value getValue() { return value; }
        public void setValue(Value value) { this.value = value; }
    }

    /**
     * 热门商品内部类
     */
    public static class HotProduct {
        private int cell_type;
        private List<Product> product_list;

        public int getCell_type() { return cell_type; }
        public void setCell_type(int cell_type) { this.cell_type = cell_type; }

        public List<Product> getProduct_list() { return product_list; }
        public void setProduct_list(List<Product> product_list) { this.product_list = product_list; }
    }

    /**
     * 店铺展示商品内部类
     */
    public static class ShopShowProduct {
        private int cell_type;
        // 根据实际数据结构添加字段

        public int getCell_type() { return cell_type; }
        public void setCell_type(int cell_type) { this.cell_type = cell_type; }
    }

    /**
     * 指标数值内部类
     */
    public static class IndexValues {
        private Value value;
        private Value out_period_ratio;
        private ExtraValue extra_value;

        // Getters and Setters
        public Value getValue() { return value; }
        public void setValue(Value value) { this.value = value; }

        public Value getOut_period_ratio() { return out_period_ratio; }
        public void setOut_period_ratio(Value out_period_ratio) { this.out_period_ratio = out_period_ratio; }

        public ExtraValue getExtra_value() { return extra_value; }
        public void setExtra_value(ExtraValue extra_value) { this.extra_value = extra_value; }
        
        public String getBestValue() { 
            Value v = getBestValue0();
            return value2String(v);
        }
        
        public static String value2String(Value v) {
            if(v == null) return null;
            
            if(StringUtils.isNotBlank(v.getValue_str())) {
                return v.getValue_str();
            }
            return String.valueOf(v.getValue());
        }
        
        private Value getBestValue0() {
            if(value != null) {
                return value; 
            }
            if(extra_value != null) {
                if(extra_value.getLower() != null) {
                    return extra_value.getLower();
                }
                if(extra_value.getUpper() != null) {
                    return extra_value.getUpper();
                }
            }
            return null;
        }
    }

    /**
     * 数值内部类
     */
    public static class Value {
        private int unit;
        private double value;
        private String value_str;

        // Getters and Setters
        public int getUnit() { return unit; }
        public void setUnit(int unit) { this.unit = unit; }

        public double getValue() { return value; }
        public void setValue(double value) { this.value = value; }

        public String getValue_str() { return value_str; }
        public void setValue_str(String value_str) { this.value_str = value_str; }
    }

    /**
     * 范围值内部类
     */
    public static class ExtraValue {
        private Value lower;
        private Value upper;

        // Getters and Setters
        public Value getLower() { return lower; }
        public void setLower(Value lower) { this.lower = lower; }

        public Value getUpper() { return upper; }
        public void setUpper(Value upper) { this.upper = upper; }
    }

    /**
     * 商品信息内部类
     */
    public static class Product {
        private String detail_h5_url;
        private int product_audit_status;
        private String product_id;
        private String product_image;
        private String product_name;
        private int product_status;

        // Getters and Setters
        public String getDetail_h5_url() { return detail_h5_url; }
        public void setDetail_h5_url(String detail_h5_url) { this.detail_h5_url = detail_h5_url; }

        public int getProduct_audit_status() { return product_audit_status; }
        public void setProduct_audit_status(int product_audit_status) { this.product_audit_status = product_audit_status; }

        public String getProduct_id() { return product_id; }
        public void setProduct_id(String product_id) { this.product_id = product_id; }

        public String getProduct_image() { return product_image; }
        public void setProduct_image(String product_image) { this.product_image = product_image; }

        public String getProduct_name() { return product_name; }
        public void setProduct_name(String product_name) { this.product_name = product_name; }

        public int getProduct_status() { return product_status; }
        public void setProduct_status(int product_status) { this.product_status = product_status; }
    }
}