package com.alibaba.es.demo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IndexRequestDemo {
    public static void main(String[] args) throws Exception {
        //RestHighLevelClient
        RestHighLevelClient client = InitRestHighLevelClient.getClient();

        //JSON
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        IndexRequest requestJson = new IndexRequest("posts").source(jsonString, XContentType.JSON);

        //Map
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequestMap = new IndexRequest("posts").id("1").source(jsonMap);

        //XContentBuilder
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            //builder.timeValueField("postDate", "", new Date().getTime());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequestBuilder = new IndexRequest("posts").id("1").source(builder);

        //Object key-pairs
        IndexRequest indexRequest = new IndexRequest("posts").id("1")
                .source(
                        "user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch"
                );
      /*
      Optional arguments:可选参数

        private String type;
        private String id;
        private BytesReference source;

        private String routing;
        private TimeValue ttl;
        private String parent;
        private String timestamp;
        private long version;
        private VersionType versionType;
        private String pipeline;
        private DocWriteRequest.OpType opType;
       */
        //routing
//        requestJson.routing("routing");
        //timeout
//        requestJson.timeout(TimeValue.timeValueSeconds(1));
//        requestJson.timeout("1s");
        //Refresh policy
//        requestJson.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
//        requestJson.setRefreshPolicy("wait_for");
        //version
//        requestJson.version(1);
        //versionType
//        requestJson.versionType(VersionType.EXTERNAL);
        //Operation type
//        requestJson.opType(DocWriteRequest.OpType.CREATE);
//        requestJson.opType("create");
        //Pipeline
//        requestJson.setPipeline("pipeline");
        //Synchronous 同步    在无法解析响应数据、请求超时或类似的情况下(服务器没有返回响应)，同步调用可能会抛出IOException
        //在服务器返回4xx或5xx错误代码的情况下，客户端尝试解析响应体错误细节，然后抛出通用的ElasticsearchException，并添加原始的ResponseException作为抑制异常。
        /*IndexResponse indexResponse = client.index(requestJson, RequestOptions.DEFAULT);
        //处理响应结果
        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }*/

        //Asynchronous 异步情况
        //执行IndexRequest也可以以异步方式完成，这样客户端就可以直接返回。用户需要通过将请求和侦听器传递给异步索引方法来指定如何处理响应或潜在故障:
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                //处理成功响应
                String index = indexResponse.getIndex();
                String id = indexResponse.getId();
                System.out.printf("index:"+index+"\t,id:"+id);
            }

            @Override
            public void onFailure(Exception e) {
                //处理失败响应
                System.out.println("创建索引异常：" + e.getMessage());
            }
        };
        client.indexAsync(requestJson, RequestOptions.DEFAULT, listener);
    }
}
