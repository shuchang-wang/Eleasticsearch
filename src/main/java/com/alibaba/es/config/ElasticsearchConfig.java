package com.alibaba.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${alibaba.elasticsearch.hostlist}")
    private String hostlist;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() {
        String[] hosts = hostlist.split(",");
        HttpHost[] hostArray = new HttpHost[hosts.length];
        for (int i = 0; i < hostArray.length; i++) {
            String item = hosts[i];
            hostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }
        return new RestHighLevelClient(RestClient.builder(hostArray));
    }
}