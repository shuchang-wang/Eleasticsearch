package com.alibaba.es.domain;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class CourseSearchParam {
    //关键字
    private String keyword;

    //一级分类
    private String mt;

    //二级分类
    private String st;

    //难度等级
    private String grade;

    //价格区间
    private Float price_min;
    private Float price_max;

    //排序字段
    private String sort;

    //过虑字段
    private String filter;
}
