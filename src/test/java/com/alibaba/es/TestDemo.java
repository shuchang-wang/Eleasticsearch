package com.alibaba.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class TestDemo {
    public static void main(String[] args) throws IOException {
        // 1 获取连接客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("127.0.0.1", 9200, "http")
        ));
        //​ 2构建请求
        GetRequest request = new GetRequest("book", "1");
        //​ 3执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //​ 4获取结果
        if (response.isExists()) {
            System.out.println(response.getIndex());
            System.out.println(response.getType());
            System.out.println(response.getId());
            System.out.println(response.getVersion());
            System.out.println(response.getSeqNo());
            System.out.println(response.getPrimaryTerm());
            System.out.println(response.getSourceAsString());
        }
    }
}