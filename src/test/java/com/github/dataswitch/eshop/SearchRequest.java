package com.github.dataswitch.eshop;

import java.util.List;

public class SearchRequest {
    private Condition condition;
    private String clue_type;
    private Integer clue_type_new;
    private Page page;
    private Integer terminal_type;
    private String source;

    // 静态工厂方法，使用提供的JSON值创建对象
    public static SearchRequest createWithDefaultValues() {
        SearchRequest request = new SearchRequest();
        
        // 创建并设置Condition对象
        Condition condition = new Condition();
        condition.setClue_info("");
        condition.setRecently_created(false);
        
        // 创建并设置Sort对象
        Sort sort = new Sort();
        sort.setSort_direction(1);
        sort.setSort_field("MATCH_DEGREE");
        condition.setSort(sort);
        
        condition.setCategory_qualification(false);
        condition.setCategory_clue_auto_submit(false);
        condition.setTag_id_list(List.of());
        condition.setProfit_id_list(List.of());
        condition.setClue_attr_key_list(List.of());
        condition.setBenefit_crowd_group(List.of());
        condition.setBenefit_content_type(List.of());
        condition.setAttr_values(List.of());
        condition.setHit_clue_label_ext(true);
        condition.setShow_new_supply_link(true);
        condition.setInclude_hot_sales_products(true);
        
        request.setCondition(condition);
        request.setClue_type("");
        request.setClue_type_new(11);
        
        // 创建并设置Page对象
        Page page = new Page();
        page.setPage_size(20);
        page.setCurrent(1);
        request.setPage(page);
        
        request.setTerminal_type(0);
        request.setSource("business_center");
        
        return request;
    }

    // Getters and Setters
    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public String getClue_type() {
        return clue_type;
    }

    public void setClue_type(String clue_type) {
        this.clue_type = clue_type;
    }

    public Integer getClue_type_new() {
        return clue_type_new;
    }

    public void setClue_type_new(Integer clue_type_new) {
        this.clue_type_new = clue_type_new;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public Integer getTerminal_type() {
        return terminal_type;
    }

    public void setTerminal_type(Integer terminal_type) {
        this.terminal_type = terminal_type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public static class Condition {
        private String clue_info;
        private Boolean recently_created;
        private Sort sort;
        private Boolean category_qualification;
        private Boolean category_clue极_auto_submit;
        private List<Object> tag_id_list;
        private List<Object> profit_id_list;
        private List<Object> clue_attr_key_list;
        private List<Object> benefit_crowd_group;
        private List<Object> benefit_content_type;
        private List<Object> attr_values;
        private Boolean hit_clue_label_ext;
        private Boolean show_new_supply_link;
        private Boolean include_hot_sales_products;
        private Boolean category_clue_auto_submit;

        // Getters and Setters
        public String getClue_info() {
            return clue_info;
        }

        public void setClue_info(String clue_info) {
            this.clue_info = clue_info;
        }

        public Boolean getRecently_created() {
            return recently_created;
        }

        public void setRecently_created(Boolean recently_created) {
            this.recently_created = recently_created;
        }

        public Sort getSort() {
            return sort;
        }

        public void setSort(Sort sort) {
            this.sort = sort;
        }

        public Boolean getCategory_qualification() {
            return category_qualification;
        }

        public void setCategory_qualification(Boolean category_qualification) {
            this.category_qualification = category_qualification;
        }

        public Boolean getCategory_clue_auto_submit() {
            return category_clue_auto_submit;
        }

        public void setCategory_clue_auto_submit(Boolean category_clue_auto_submit) {
            this.category_clue_auto_submit = category_clue_auto_submit;
        }

        public List<Object> getTag_id_list() {
            return tag_id_list;
        }

        public void setTag_id_list(List<Object> tag_id_list) {
            this.tag_id_list = tag_id_list;
        }

        public List<Object> getProfit_id_list() {
            return profit_id_list;
        }

        public void setProfit_id_list(List<Object> profit_id_list) {
            this.profit_id_list = profit_id_list;
        }

        public List<Object> getClue_attr_key_list() {
            return clue_attr_key_list;
        }

        public void setClue_attr_key_list(List<Object> clue_attr_key_list) {
            this.clue_attr_key_list = clue_attr_key_list;
        }

        public List<Object> getBenefit_crowd_group() {
            return benefit_crowd_group;
        }

        public void setBenefit_crowd_group(List<Object> benefit_crowd_group) {
            this.benefit_crowd_group = benefit_crowd_group;
        }

        public List<Object> getBenefit_content_type() {
            return benefit_content_type;
        }

        public void setBenefit_content_type(List<Object> benefit_content_type) {
            this.benefit_content_type = benefit_content_type;
        }

        public List<Object> getAttr_values() {
            return attr_values;
        }

        public void setAttr_values(List<Object> attr_values) {
            this.attr_values = attr_values;
        }

        public Boolean getHit_clue_label_ext() {
            return hit_clue_label_ext;
        }

        public void setHit_clue_label_ext(Boolean hit_clue_label_ext) {
            this.hit_clue_label_ext = hit_clue_label_ext;
        }

        public Boolean getShow_new_supply_link() {
            return show_new_supply_link;
        }

        public void setShow_new_supply_link(Boolean show_new_supply_link) {
            this.show_new_supply_link = show_new_supply_link;
        }

        public Boolean getInclude_hot_sales_products() {
            return include_hot_sales_products;
        }

        public void setInclude_hot_sales_products(Boolean include_hot_sales_products) {
            this.include_hot_sales_products = include_hot_sales_products;
        }
    }

    public static class Sort {
        private Integer sort_direction;
        private String sort_field;

        // Getters and Setters
        public Integer getSort_direction() {
            return sort_direction;
        }

        public void setSort_direction(Integer sort_direction) {
            this.sort_direction = sort_direction;
        }

        public String getSort_field() {
            return sort_field;
        }

        public void setSort_field(String sort_field) {
            this.sort_field = sort_field;
        }
    }

    public static class Page {
        private Integer page_size;
        private Integer current;

        // Getters and Setters
        public Integer getPage_size() {
            return page_size;
        }

        public void setPage_size(Integer page_size) {
            this.page_size = page_size;
        }

        public Integer getCurrent() {
            return current;
        }

        public void setCurrent(Integer current) {
            this.current = current;
        }
    }
}