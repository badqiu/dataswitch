package com.github.dataswitch.eshop;
import java.util.Date;
import java.util.List;

/**
 * 抖店商机中心数据
 */
public class ClueData {
    public ClueDetail clue_detail;
    public int submit_times;
    public boolean is_collect;
    public boolean is_grant;
    public QueryClueCardInfo query_clue_card_info;
    public Object product_diagnose_result; // 使用Object处理null值
    public Object auto_submit_task;        // 使用Object处理null值
    public boolean cate_qualification_open;
    public boolean has_cate_qualification;
    public Object word_clue_indicator_info; // 使用Object处理null值
    public Object category_clue_extra_info; // 使用Object处理null值
    public List<HotSaleProduct> hot_sale_products;
    public ClueIndicator clue_indicator;
    public ClueCollectInfo clue_collect_info;
    
    public Date batch_date;
    
    

    public ClueDetail getClue_detail() {
		return clue_detail;
	}

	public void setClue_detail(ClueDetail clue_detail) {
		this.clue_detail = clue_detail;
	}

	public int getSubmit_times() {
		return submit_times;
	}

	public void setSubmit_times(int submit_times) {
		this.submit_times = submit_times;
	}

	public boolean isIs_collect() {
		return is_collect;
	}

	public void setIs_collect(boolean is_collect) {
		this.is_collect = is_collect;
	}

	public boolean isIs_grant() {
		return is_grant;
	}

	public void setIs_grant(boolean is_grant) {
		this.is_grant = is_grant;
	}

	public QueryClueCardInfo getQuery_clue_card_info() {
		return query_clue_card_info;
	}

	public void setQuery_clue_card_info(QueryClueCardInfo query_clue_card_info) {
		this.query_clue_card_info = query_clue_card_info;
	}

	public Object getProduct_diagnose_result() {
		return product_diagnose_result;
	}

	public void setProduct_diagnose_result(Object product_diagnose_result) {
		this.product_diagnose_result = product_diagnose_result;
	}

	public Object getAuto_submit_task() {
		return auto_submit_task;
	}

	public void setAuto_submit_task(Object auto_submit_task) {
		this.auto_submit_task = auto_submit_task;
	}

	public boolean isCate_qualification_open() {
		return cate_qualification_open;
	}

	public void setCate_qualification_open(boolean cate_qualification_open) {
		this.cate_qualification_open = cate_qualification_open;
	}

	public boolean isHas_cate_qualification() {
		return has_cate_qualification;
	}

	public void setHas_cate_qualification(boolean has_cate_qualification) {
		this.has_cate_qualification = has_cate_qualification;
	}

	public Object getWord_clue_indicator_info() {
		return word_clue_indicator_info;
	}

	public void setWord_clue_indicator_info(Object word_clue_indicator_info) {
		this.word_clue_indicator_info = word_clue_indicator_info;
	}

	public Object getCategory_clue_extra_info() {
		return category_clue_extra_info;
	}

	public void setCategory_clue_extra_info(Object category_clue_extra_info) {
		this.category_clue_extra_info = category_clue_extra_info;
	}

	public List<HotSaleProduct> getHot_sale_products() {
		return hot_sale_products;
	}

	public void setHot_sale_products(List<HotSaleProduct> hot_sale_products) {
		this.hot_sale_products = hot_sale_products;
	}

	public ClueIndicator getClue_indicator() {
		return clue_indicator;
	}

	public void setClue_indicator(ClueIndicator clue_indicator) {
		this.clue_indicator = clue_indicator;
	}

	public ClueCollectInfo getClue_collect_info() {
		return clue_collect_info;
	}

	public void setClue_collect_info(ClueCollectInfo clue_collect_info) {
		this.clue_collect_info = clue_collect_info;
	}
	
	public Date getBatch_date() {
		return batch_date;
	}

	public void setBatch_date(Date batch_date) {
		this.batch_date = batch_date;
	}



	// 嵌套实体类定义
    public static class ClueDetail {
        public long clue_id;
        public String name;
        public int first_cid;
        public String first_name;
        public int second_cid;
        public String second_name;
        public int brand_id;
        public String brand_name;
        public double price_min;
        public double price_max;
        public String product_pic_url;
        public String clue_channel;
        public String brand_name_en;
        public int clue_status;
        public double gmv;
        public int order_num;
        public String category_name;
        public List<String> pic_url_list;
        public boolean urgent_recruitment_tag;
        public int third_cid;
        public String third_name;
        public Integer fourth_cid;       // 使用包装类处理null值
        public String fourth_name;       // 使用包装类处理null值
        public List<String> category_path;
        public List<ClueLabel> clue_label_list;
        public List<ProfitInfo> profit_info_list;
        public int category_id;
        public List<Object> price_list;  // 使用Object处理null值
        public List<Object> clue_attr_list;
        public boolean is_sale_promotion_clue;
        public List<Object> clue_submit_requirements;
        public String platform_id;
        public String short_name;
		public long getClue_id() {
			return clue_id;
		}
		public void setClue_id(long clue_id) {
			this.clue_id = clue_id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getFirst_cid() {
			return first_cid;
		}
		public void setFirst_cid(int first_cid) {
			this.first_cid = first_cid;
		}
		public String getFirst_name() {
			return first_name;
		}
		public void setFirst_name(String first_name) {
			this.first_name = first_name;
		}
		public int getSecond_cid() {
			return second_cid;
		}
		public void setSecond_cid(int second_cid) {
			this.second_cid = second_cid;
		}
		public String getSecond_name() {
			return second_name;
		}
		public void setSecond_name(String second_name) {
			this.second_name = second_name;
		}
		public int getBrand_id() {
			return brand_id;
		}
		public void setBrand_id(int brand_id) {
			this.brand_id = brand_id;
		}
		public String getBrand_name() {
			return brand_name;
		}
		public void setBrand_name(String brand_name) {
			this.brand_name = brand_name;
		}
		public double getPrice_min() {
			return price_min;
		}
		public void setPrice_min(double price_min) {
			this.price_min = price_min;
		}
		public double getPrice_max() {
			return price_max;
		}
		public void setPrice_max(double price_max) {
			this.price_max = price_max;
		}
		public String getProduct_pic_url() {
			return product_pic_url;
		}
		public void setProduct_pic_url(String product_pic_url) {
			this.product_pic_url = product_pic_url;
		}
		public String getClue_channel() {
			return clue_channel;
		}
		public void setClue_channel(String clue_channel) {
			this.clue_channel = clue_channel;
		}
		public String getBrand_name_en() {
			return brand_name_en;
		}
		public void setBrand_name_en(String brand_name_en) {
			this.brand_name_en = brand_name_en;
		}
		public int getClue_status() {
			return clue_status;
		}
		public void setClue_status(int clue_status) {
			this.clue_status = clue_status;
		}
		public double getGmv() {
			return gmv;
		}
		public void setGmv(double gmv) {
			this.gmv = gmv;
		}
		public int getOrder_num() {
			return order_num;
		}
		public void setOrder_num(int order_num) {
			this.order_num = order_num;
		}
		public String getCategory_name() {
			return category_name;
		}
		public void setCategory_name(String category_name) {
			this.category_name = category_name;
		}
		public List<String> getPic_url_list() {
			return pic_url_list;
		}
		public void setPic_url_list(List<String> pic_url_list) {
			this.pic_url_list = pic_url_list;
		}
		public boolean isUrgent_recruitment_tag() {
			return urgent_recruitment_tag;
		}
		public void setUrgent_recruitment_tag(boolean urgent_recruitment_tag) {
			this.urgent_recruitment_tag = urgent_recruitment_tag;
		}
		public int getThird_cid() {
			return third_cid;
		}
		public void setThird_cid(int third_cid) {
			this.third_cid = third_cid;
		}
		public String getThird_name() {
			return third_name;
		}
		public void setThird_name(String third_name) {
			this.third_name = third_name;
		}
		public Integer getFourth_cid() {
			return fourth_cid;
		}
		public void setFourth_cid(Integer fourth_cid) {
			this.fourth_cid = fourth_cid;
		}
		public String getFourth_name() {
			return fourth_name;
		}
		public void setFourth_name(String fourth_name) {
			this.fourth_name = fourth_name;
		}
		public List<String> getCategory_path() {
			return category_path;
		}
		public void setCategory_path(List<String> category_path) {
			this.category_path = category_path;
		}
		public List<ClueLabel> getClue_label_list() {
			return clue_label_list;
		}
		public void setClue_label_list(List<ClueLabel> clue_label_list) {
			this.clue_label_list = clue_label_list;
		}
		public List<ProfitInfo> getProfit_info_list() {
			return profit_info_list;
		}
		public void setProfit_info_list(List<ProfitInfo> profit_info_list) {
			this.profit_info_list = profit_info_list;
		}
		public int getCategory_id() {
			return category_id;
		}
		public void setCategory_id(int category_id) {
			this.category_id = category_id;
		}
		public List<Object> getPrice_list() {
			return price_list;
		}
		public void setPrice_list(List<Object> price_list) {
			this.price_list = price_list;
		}
		public List<Object> getClue_attr_list() {
			return clue_attr_list;
		}
		public void setClue_attr_list(List<Object> clue_attr_list) {
			this.clue_attr_list = clue_attr_list;
		}
		public boolean isIs_sale_promotion_clue() {
			return is_sale_promotion_clue;
		}
		public void setIs_sale_promotion_clue(boolean is_sale_promotion_clue) {
			this.is_sale_promotion_clue = is_sale_promotion_clue;
		}
		public List<Object> getClue_submit_requirements() {
			return clue_submit_requirements;
		}
		public void setClue_submit_requirements(List<Object> clue_submit_requirements) {
			this.clue_submit_requirements = clue_submit_requirements;
		}
		public String getPlatform_id() {
			return platform_id;
		}
		public void setPlatform_id(String platform_id) {
			this.platform_id = platform_id;
		}
		public String getShort_name() {
			return short_name;
		}
		public void setShort_name(String short_name) {
			this.short_name = short_name;
		}
        
        
    }

    public static class ClueLabel {
        public int label_id;
        public String label_name;
        public String label_desc;
        public int label_priority;
        public ClueLabelCategoryInfo clue_label_category_info;
        public int rank_level;
        public String top_rank_label_text;
        public String top_rank_label_hover_text;
        public boolean is_top_level;
        public boolean is_explosive;
		public int getLabel_id() {
			return label_id;
		}
		public void setLabel_id(int label_id) {
			this.label_id = label_id;
		}
		public String getLabel_name() {
			return label_name;
		}
		public void setLabel_name(String label_name) {
			this.label_name = label_name;
		}
		public String getLabel_desc() {
			return label_desc;
		}
		public void setLabel_desc(String label_desc) {
			this.label_desc = label_desc;
		}
		public int getLabel_priority() {
			return label_priority;
		}
		public void setLabel_priority(int label_priority) {
			this.label_priority = label_priority;
		}
		public ClueLabelCategoryInfo getClue_label_category_info() {
			return clue_label_category_info;
		}
		public void setClue_label_category_info(ClueLabelCategoryInfo clue_label_category_info) {
			this.clue_label_category_info = clue_label_category_info;
		}
		public int getRank_level() {
			return rank_level;
		}
		public void setRank_level(int rank_level) {
			this.rank_level = rank_level;
		}
		public String getTop_rank_label_text() {
			return top_rank_label_text;
		}
		public void setTop_rank_label_text(String top_rank_label_text) {
			this.top_rank_label_text = top_rank_label_text;
		}
		public String getTop_rank_label_hover_text() {
			return top_rank_label_hover_text;
		}
		public void setTop_rank_label_hover_text(String top_rank_label_hover_text) {
			this.top_rank_label_hover_text = top_rank_label_hover_text;
		}
		public boolean isIs_top_level() {
			return is_top_level;
		}
		public void setIs_top_level(boolean is_top_level) {
			this.is_top_level = is_top_level;
		}
		public boolean isIs_explosive() {
			return is_explosive;
		}
		public void setIs_explosive(boolean is_explosive) {
			this.is_explosive = is_explosive;
		}
        
        
    }

    public static class ClueLabelCategoryInfo {
        public int category_id;
        public String category_name;
        public int category_priority;
        public int level;
        public int parent_id;
        public Object parent;  // 使用Object处理null值
		public int getCategory_id() {
			return category_id;
		}
		public void setCategory_id(int category_id) {
			this.category_id = category_id;
		}
		public String getCategory_name() {
			return category_name;
		}
		public void setCategory_name(String category_name) {
			this.category_name = category_name;
		}
		public int getCategory_priority() {
			return category_priority;
		}
		public void setCategory_priority(int category_priority) {
			this.category_priority = category_priority;
		}
		public int getLevel() {
			return level;
		}
		public void setLevel(int level) {
			this.level = level;
		}
		public int getParent_id() {
			return parent_id;
		}
		public void setParent_id(int parent_id) {
			this.parent_id = parent_id;
		}
		public Object getParent() {
			return parent;
		}
		public void setParent(Object parent) {
			this.parent = parent;
		}
        
        
    }

    public static class ProfitInfo {
        public int profit_id;
        public String profit_name;
        public int profit_active_status;
        public Object boosted_start_time;  // 使用Object处理null值
        public Object boosted_end_time;    // 使用Object处理null值
        public String boosted_status_name;
        public String boosted_status_desc;
        public Object profit_acquisition_rule_list;
        public Object profit_diagnose_ret_list;
        public Object profit_end_time;
        public boolean only_show_support_text;
        public String support_text;
        public int support_action;
        public Object complex_profit_diagnotor_ret_list;
        public String competitive_price_ref_product_id;
        public boolean show_text_and_chart_line;
        public String link_str;
        public String support_action_name;
        public String support_action_url;
        public Object product_card_info_list;
    }

    public static class QueryClueCardInfo {
        public int search_popularity;
        public int out_of_stock_level;
        public Object search_increase;      // 使用Object处理null值
        public int trend_hot;
        public Object goods_supply_exists;  // 使用Object处理null值
        public String query_id;
        public double demand_supply_rate;
        public int related_product_cnt;
        public boolean hot;
        public int qnl_clue_type;
        public List<Object> attr_list;
        public List<Integer> goods_supply_platform_list;
        public int industry_selected_clue_type;
    }

    public static class HotSaleProduct {
        public String prod_id;
        public String prod_name;
        public String prod_image;
        public int prod_saled;
        public double prod_price;
        public String prod_qr_code;
		public String getProd_id() {
			return prod_id;
		}
		public void setProd_id(String prod_id) {
			this.prod_id = prod_id;
		}
		public String getProd_name() {
			return prod_name;
		}
		public void setProd_name(String prod_name) {
			this.prod_name = prod_name;
		}
		public String getProd_image() {
			return prod_image;
		}
		public void setProd_image(String prod_image) {
			this.prod_image = prod_image;
		}
		public int getProd_saled() {
			return prod_saled;
		}
		public void setProd_saled(int prod_saled) {
			this.prod_saled = prod_saled;
		}
		public double getProd_price() {
			return prod_price;
		}
		public void setProd_price(double prod_price) {
			this.prod_price = prod_price;
		}
		public String getProd_qr_code() {
			return prod_qr_code;
		}
		public void setProd_qr_code(String prod_qr_code) {
			this.prod_qr_code = prod_qr_code;
		}
        
        
    }

    public static class ClueIndicator {
        public double search_heat;
        public Object demand_heat_rate;  // 使用Object处理null值
        public double demand_supply_rate;
        public Object demand_supply_rate_30d_rate;
        public double pay_amount_ind;
        public int pay_amount_ind_30d_rate;
        public int online_prod_cnt;
        public Object online_prod_cnt_30d_rate;
        public Object online_shop_cnt;
        public Object online_shop_cnt_30d_rate;
        public int search_heat_rank;
        public int demand_and_supply_rate_rank;
        public List<Object> clue_Label_category_rank;
        public Object pay_amount_int_rank;
        public String pay_amount_ind_range;
        public String online_prod_cnt_range;
        public Object online_shop_cnt_range;
        public String demand_heat_range;
        public String pay_order_cnt_range;
        public int pay_order_cnt;
        public int search_pv_cnt;
        public String search_pv_cnt_range;
        public int search_pv_cnt_30d_rate;
		public double getSearch_heat() {
			return search_heat;
		}
		public void setSearch_heat(double search_heat) {
			this.search_heat = search_heat;
		}
		public Object getDemand_heat_rate() {
			return demand_heat_rate;
		}
		public void setDemand_heat_rate(Object demand_heat_rate) {
			this.demand_heat_rate = demand_heat_rate;
		}
		public double getDemand_supply_rate() {
			return demand_supply_rate;
		}
		public void setDemand_supply_rate(double demand_supply_rate) {
			this.demand_supply_rate = demand_supply_rate;
		}
		public Object getDemand_supply_rate_30d_rate() {
			return demand_supply_rate_30d_rate;
		}
		public void setDemand_supply_rate_30d_rate(Object demand_supply_rate_30d_rate) {
			this.demand_supply_rate_30d_rate = demand_supply_rate_30d_rate;
		}
		public double getPay_amount_ind() {
			return pay_amount_ind;
		}
		public void setPay_amount_ind(double pay_amount_ind) {
			this.pay_amount_ind = pay_amount_ind;
		}
		public int getPay_amount_ind_30d_rate() {
			return pay_amount_ind_30d_rate;
		}
		public void setPay_amount_ind_30d_rate(int pay_amount_ind_30d_rate) {
			this.pay_amount_ind_30d_rate = pay_amount_ind_30d_rate;
		}
		public int getOnline_prod_cnt() {
			return online_prod_cnt;
		}
		public void setOnline_prod_cnt(int online_prod_cnt) {
			this.online_prod_cnt = online_prod_cnt;
		}
		public Object getOnline_prod_cnt_30d_rate() {
			return online_prod_cnt_30d_rate;
		}
		public void setOnline_prod_cnt_30d_rate(Object online_prod_cnt_30d_rate) {
			this.online_prod_cnt_30d_rate = online_prod_cnt_30d_rate;
		}
		public Object getOnline_shop_cnt() {
			return online_shop_cnt;
		}
		public void setOnline_shop_cnt(Object online_shop_cnt) {
			this.online_shop_cnt = online_shop_cnt;
		}
		public Object getOnline_shop_cnt_30d_rate() {
			return online_shop_cnt_30d_rate;
		}
		public void setOnline_shop_cnt_30d_rate(Object online_shop_cnt_30d_rate) {
			this.online_shop_cnt_30d_rate = online_shop_cnt_30d_rate;
		}
		public int getSearch_heat_rank() {
			return search_heat_rank;
		}
		public void setSearch_heat_rank(int search_heat_rank) {
			this.search_heat_rank = search_heat_rank;
		}
		public int getDemand_and_supply_rate_rank() {
			return demand_and_supply_rate_rank;
		}
		public void setDemand_and_supply_rate_rank(int demand_and_supply_rate_rank) {
			this.demand_and_supply_rate_rank = demand_and_supply_rate_rank;
		}
		public List<Object> getClue_Label_category_rank() {
			return clue_Label_category_rank;
		}
		public void setClue_Label_category_rank(List<Object> clue_Label_category_rank) {
			this.clue_Label_category_rank = clue_Label_category_rank;
		}
		public Object getPay_amount_int_rank() {
			return pay_amount_int_rank;
		}
		public void setPay_amount_int_rank(Object pay_amount_int_rank) {
			this.pay_amount_int_rank = pay_amount_int_rank;
		}
		public String getPay_amount_ind_range() {
			return pay_amount_ind_range;
		}
		public void setPay_amount_ind_range(String pay_amount_ind_range) {
			this.pay_amount_ind_range = pay_amount_ind_range;
		}
		public String getOnline_prod_cnt_range() {
			return online_prod_cnt_range;
		}
		public void setOnline_prod_cnt_range(String online_prod_cnt_range) {
			this.online_prod_cnt_range = online_prod_cnt_range;
		}
		public Object getOnline_shop_cnt_range() {
			return online_shop_cnt_range;
		}
		public void setOnline_shop_cnt_range(Object online_shop_cnt_range) {
			this.online_shop_cnt_range = online_shop_cnt_range;
		}
		public String getDemand_heat_range() {
			return demand_heat_range;
		}
		public void setDemand_heat_range(String demand_heat_range) {
			this.demand_heat_range = demand_heat_range;
		}
		public String getPay_order_cnt_range() {
			return pay_order_cnt_range;
		}
		public void setPay_order_cnt_range(String pay_order_cnt_range) {
			this.pay_order_cnt_range = pay_order_cnt_range;
		}
		public int getPay_order_cnt() {
			return pay_order_cnt;
		}
		public void setPay_order_cnt(int pay_order_cnt) {
			this.pay_order_cnt = pay_order_cnt;
		}
		public int getSearch_pv_cnt() {
			return search_pv_cnt;
		}
		public void setSearch_pv_cnt(int search_pv_cnt) {
			this.search_pv_cnt = search_pv_cnt;
		}
		public String getSearch_pv_cnt_range() {
			return search_pv_cnt_range;
		}
		public void setSearch_pv_cnt_range(String search_pv_cnt_range) {
			this.search_pv_cnt_range = search_pv_cnt_range;
		}
		public int getSearch_pv_cnt_30d_rate() {
			return search_pv_cnt_30d_rate;
		}
		public void setSearch_pv_cnt_30d_rate(int search_pv_cnt_30d_rate) {
			this.search_pv_cnt_30d_rate = search_pv_cnt_30d_rate;
		}
        
        
    }

    public static class ClueCollectInfo {
        public int collect_status;
        public String collect_tips;
		public int getCollect_status() {
			return collect_status;
		}
		public void setCollect_status(int collect_status) {
			this.collect_status = collect_status;
		}
		public String getCollect_tips() {
			return collect_tips;
		}
		public void setCollect_tips(String collect_tips) {
			this.collect_tips = collect_tips;
		}
        
        
    }
    
    
}

