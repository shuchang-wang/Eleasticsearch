package com.alibaba.es.controller;

import com.alibaba.es.domain.CoursePub;
import com.alibaba.es.domain.CourseSearchParam;
import com.alibaba.es.domain.QueryResponseResult;
import com.alibaba.es.service.EsCourseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author WSC
 * @version 1.0
 **/
@RestController
@RequestMapping("/search/course")
public class EsCourseController {

    @Autowired
    EsCourseService esCourseService;

    @GetMapping(value = "/list/{page}/{size}")
    public QueryResponseResult<CoursePub> list(@PathVariable("page") int page, @PathVariable("size") int size, CourseSearchParam courseSearchParam) {
        return esCourseService.list(page, size, courseSearchParam);
    }

}