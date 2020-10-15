package com.alibaba.es;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearch {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //搜索全部记录
    @Test
    public void searchTest() throws IOException {
        //        GET /book/_search
        //        {
        //          "query": {
        //            "match_all": {}
        //          }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //获取某些字段
        searchSourceBuilder.fetchSource(new String[]{"name"}, new String[]{});

        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();

        SearchHit[] hits1 = hits.getHits();
        for (SearchHit searchHit : hits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //搜索分页
    @Test
    public void testSearchPage() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //              "match_all": {}
        //            },
        //            "from": 0,
        //            "size": 2
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //第几页
        int page = 1;
        //每页显示的条数
        int size = 2;
        //下标计算
        int from = (page - 1) * size;

        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //搜索id
    @Test
    public void testSearchByIds() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //              "ids" : {
        //                   "values" : ["1", "3"]
        //              }
        //          }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds("1", "4", "100"));
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //match搜索
    @Test
    public void testSearchMatch() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //              "match": {
        //                "description": "java"
        //              }
        //          }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("description", "java程序员"));
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //term搜索
    @Test
    public void testSearchTerm() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //              "term": {
        //                "description": "java程序员"
        //              }
        //          }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("description", "java程序员"));
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //multiMatch搜索
    @Test
    public void testSearchMultiMatch() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //                "multi_match": {
        //                    "query": "java程序员",
        //                    "fields": ["name", "description"]
        //                }
        //            }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("java程序员", "name", "description"));
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //boolquery搜索
    @Test
    public void testSearchBool() throws IOException {
        //        GET /book/_search
        //        {
        //            "query": {
        //                "bool": {
        //                    "must": [
        //                        {
        //                            "multi_match": {
        //                            "query": "java程序员",
        //                                    "fields": ["name", "description"]
        //                        }
        //                        }
        //                     ],
        //                    "should": [
        //                        {
        //                            "match": {
        //                            "studymodel": "201001"
        //                        }
        //                            }
        //                    ]
        //                }
        //            }
        //        }
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构建multiMatch请求
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("java程序员", "name", "description");
        //构建match请求
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("studymodel", "201001");
        //构建boolquery请求
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.should(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        //searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("java程序员", "name", "description")).should(QueryBuilders.matchQuery("studymodel", "201001")));
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //Filterquery搜索
    @Test
    public void testSearchFilter() throws IOException {
        //GET /book/_search
        //{
        //  "query": {
        //    "bool": {
        //      "must": [
        //        {
        //          "match": {
        //            "description": "java程序员"
        //          }
        //        }
        //      ],
        //      "filter": {
        //        "range": {
        //          "price": {
        //            "gte": 80,
        //		      "lte": 90
        //          }
        //        }
        //      }
        //    }
        //  }
        //}
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //构建match请求
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("description", "java程序员");
        //构建range请求
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").gte(80).lte(90);
        //构建boolquery请求
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(matchQueryBuilder);
        boolQueryBuilder.filter(rangeQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }

    //sort搜索
    @Test
    public void testSearchSort() throws IOException {
        //GET /book/_search
        //{
        //  "query": {
        //    "match_all": {}
        //  },
        //  "sort": [
        //    {
        //      "price": {
        //        "order": "desc"
        //      }
        //    }
        //  ]
        //}
        //构建请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        searchSourceBuilder.sort("price", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取结果
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            String id = searchHit.getId();
            float score = searchHit.getScore();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            Double price = (Double) sourceAsMap.get("price");
            System.out.println("id:" + id);
            System.out.println("name:" + name);
            System.out.println("description:" + description);
            System.out.println("price:" + price);
            System.out.println("=======================");
        }
    }
}