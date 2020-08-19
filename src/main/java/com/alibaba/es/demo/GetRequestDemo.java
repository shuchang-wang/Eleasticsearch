package com.alibaba.es.demo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GetRequestDemo {
    public static void main(String[] args) throws Exception {
        //RestHighLevelClient
        RestHighLevelClient client = InitRestHighLevelClient.getClient();

        //JSON
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        GetRequest request = new GetRequest("posts","_doc","lUsPvXIBFWMFuRBXCA7Y");

//        //Disable source retrieval, enabled by default
//        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
//        //Configure source inclusion for specific fields
//        String[] includes = new String[]{"message", "*Date"};
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext fetchSourceContext =new FetchSourceContext(true, includes, excludes);
//        request.fetchSourceContext(fetchSourceContext);
//        //Configure source exclusion for specific fields
//        String[] includes2 = Strings.EMPTY_ARRAY;
//        String[] excludes2 = new String[]{"message"};
//        FetchSourceContext fetchSourceContext2 = new FetchSourceContext(true, includes, excludes);
//        request.fetchSourceContext(fetchSourceContext);
//        //Configure retrieval for specific stored fields (requires fields to be stored separately in the mappings)
//        request.storedFields("message");
//        //Routing value
//        request.routing("routing");
//        //Preference value
//        request.preference("preference");
//        //Set realtime flag to false (true by default)
//        request.realtime(false);
//        //Perform a refresh before retrieving the document (false by default)
//        request.refresh(true);
//        //Version
//        request.version(2);
//        //Version type
//        request.versionType(VersionType.EXTERNAL);
    //同步请求
        //Synchronous 同步    在无法解析响应数据、请求超时或类似的情况下(服务器没有返回响应)，同步调用可能会抛出IOException
        //在服务器返回4xx或5xx错误代码的情况下，客户端尝试解析响应体错误细节，然后抛出通用的ElasticsearchException，并添加原始的ResponseException作为抑制异常。
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        //处理响应结果
        String index = getResponse.getIndex();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        } else {

        }
    //异步请求
        //Asynchronous 异步情况
        //执行IndexRequest也可以以异步方式完成，这样客户端就可以直接返回。用户需要通过将请求和侦听器传递给异步索引方法来指定如何处理响应或潜在故障:
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                //处理成功响应
                String index = getResponse.getIndex();
                String id = getResponse.getId();
                if (getResponse.isExists()) {
                    long version = getResponse.getVersion();
                    String sourceAsString = getResponse.getSourceAsString();
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();
                    System.out.println(index);
                    System.out.println(id);
                    System.out.println(version);
                    System.out.println(sourceAsString);
                    System.out.println(sourceAsMap);
                    System.out.println(sourceAsBytes.toString());
                }
            }

            @Override
            public void onFailure(Exception e) {
                //处理失败响应
                System.out.println("创建索引异常：" + e.getMessage());
            }
        };
        client.getAsync(request, RequestOptions.DEFAULT, listener);
    }
}
