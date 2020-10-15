package com.alibaba.es;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearch {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void searchTest(){

        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");

        //执行

        //处理结果
    }

}
