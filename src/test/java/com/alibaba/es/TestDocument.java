package com.alibaba.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(classes = SearchApplication.class)
@RunWith(SpringRunner.class)
public class TestDocument {

    @Resource
    private RestHighLevelClient client;

    @Test
    public void testGet() throws IOException {
//        1.构建请求
        GetRequest getRequest = new GetRequest("test_post", "1");
//        -------------------------可选参数start-------------------------
//为特定字段配置_source_include
//        String[] includes = {"user","message"};
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext context = new FetchSourceContext(true,includes,excludes);
//        getRequest.fetchSourceContext(context);
//为特定字段配置_source_excludes
//        String[] includes = Strings.EMPTY_ARRAY;
//        String[] excludes = {"user","message"};
//        FetchSourceContext context = new FetchSourceContext(true,includes,excludes);
//        getRequest.fetchSourceContext(context);
//        设置路由
//        getRequest.routing("routing");
//        -------------------------可选参数end-------------------------

//        2.执行
//---------------------同步查询---------------------
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
//            3.处理执行结果
//            System.out.println(getResponse.getIndex());
//            System.out.println(getResponse.getType());
//            System.out.println(getResponse.getId());
//            System.out.println(getResponse.getVersion());
//            System.out.println(getResponse.getSeqNo());
//            System.out.println(getResponse.getPrimaryTerm());
//            System.out.println(getResponse.isExists());
//            System.out.println(getResponse.getSourceAsString());

//---------------------异步查询---------------------
//        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
//            //成功时
//            public void onResponse(GetResponse getResponse) {
//                System.out.println(getResponse.getIndex());
//                System.out.println(getResponse.getType());
//                System.out.println(getResponse.getId());
//                System.out.println(getResponse.getVersion());
//                System.out.println(getResponse.getSeqNo());
//                System.out.println(getResponse.getPrimaryTerm());
//                System.out.println(getResponse.isExists());
//                System.out.println(getResponse.getSourceAsString());
//            }
//            //失败时
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//        client.getAsync(getRequest, RequestOptions.DEFAULT, listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if (getResponse.isExists()) {
            System.out.println(getResponse.getIndex());
            System.out.println(getResponse.getType());
            System.out.println(getResponse.getId());
            System.out.println(getResponse.getVersion());
            System.out.println(getResponse.getSeqNo());
            System.out.println(getResponse.getPrimaryTerm());
            System.out.println(getResponse.isExists());
            System.out.println(getResponse.getSourceAsString());//以String获取数据
            System.out.println(getResponse.getSourceAsBytes());//以bytes获取数据
            System.out.println(getResponse.getSourceAsMap());//以Map获取数据
            System.out.println(getResponse.getSourceAsMap().get("user"));//获取Map中的user数据
        } else {
            System.out.println("获取失败");
        }
    }

    @Test
    public void testAdd() throws IOException {
        //1.构建请求
        IndexRequest indexRequest = new IndexRequest("test_post");
        indexRequest.id("4");

// =================================构建文档数据=================================
        //方法一   String
        String jsonString = "{\n" +
                "  \"user\":\"tomas\",\n" +
                "  \"postDate\":\"2019-07-18\",\n" +
                "  \"message\":\"trying out es1\"\n" +
                "}";
        indexRequest.source(jsonString, XContentType.JSON);

        //方法二 Map
//        Map<String,Object> jsonMap = new HashMap<String, Object>();
//        jsonMap.put("user","tomas");
//        jsonMap.put("postDate","2019-07-18");
//        jsonMap.put("message","trying out es1");
//        indexRequest.source(jsonMap);

        //方法三 XContentBuilder
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("user","tomas");
//            builder.timeField("postDate",new Date());
//            builder.field("message","trying out es1");
//        }
//        builder.endObject();
//        indexRequest.source(builder);

        //方法四 Object...
//        indexRequest.source("user","tomas","postDate",new Date(),"message","trying out es1");

        //============================可选参数============================
        //设置超时时间
//        indexRequest.timeout("1s");
//        indexRequest.timeout(TimeValue.timeValueSeconds(1));

        //手动维护版本号
//        indexRequest.version(4);
//        indexRequest.versionType(VersionType.EXTERNAL);

        //2.执行
        //同步执行
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//        System.out.println(indexResponse.getIndex());
//        System.out.println(indexResponse.getType());
//        System.out.println(indexResponse.getId());
//        System.out.println(indexResponse.getVersion());
//        System.out.println(indexResponse.getResult());
//        System.out.println(indexResponse.getShardInfo());
//        System.out.println(indexResponse.getSeqNo());
//        System.out.println(indexResponse.getPrimaryTerm());
//        System.out.println(indexResponse.status());
//        System.out.println(indexResponse.toString());
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("CREATE：" + result);
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("UPDATE：" + result);
        } else {
            System.out.println(indexResponse.getResult());
        }

//        "_shards" : {
//            "total" : 2,
//            "successful" : 2,
//            "failed" : 0
//        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("处理成功的分片数少于总分片！");
        }

        if (shardInfo.getFailed() > 0) {
            ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
            for (ReplicationResponse.ShardInfo.Failure failure : failures) {
                String reason = failure.reason();//每一个错误的原因
                System.out.println(reason);
            }
        }
        //异步执行
//        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
//            public void onResponse(IndexResponse indexResponse) {
//                System.out.println(indexResponse.getIndex());
//                System.out.println(indexResponse.getType());
//                System.out.println(indexResponse.getId());
//                System.out.println(indexResponse.getVersion());
//                System.out.println(indexResponse.getResult());
//                System.out.println(indexResponse.getShardInfo());
//                System.out.println(indexResponse.getSeqNo());
//                System.out.println(indexResponse.getPrimaryTerm());
//                System.out.println(indexResponse.status());
//                System.out.println(indexResponse.toString());
//            }
//
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//        client.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testUpdate() throws IOException {
//        POST /test_post/_doc/3/_update  或者  POST /test_post/_update/3
//        {
//            "doc": {
//                  "user":"tomas Lee"
//              }
//        }
        //1.构建请求
        UpdateRequest updateRequest = new UpdateRequest("test_post", "4");
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("user", "tomas Lee");
        updateRequest.doc(jsonMap);

        //可选参数
        updateRequest.timeout("1s");
        updateRequest.retryOnConflict(3);//重试次数

        //2.执行
        //同步执行
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        //3.获取结果
        System.out.println(updateResponse.getIndex());
        System.out.println(updateResponse.getType());
        System.out.println(updateResponse.getId());
        System.out.println(updateResponse.getVersion());
        System.out.println(updateResponse.getSeqNo());
        System.out.println(updateResponse.getPrimaryTerm());
        System.out.println(updateResponse.getResult());
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {//创建
            System.out.println("CREATE：" + updateResponse.getResult());
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {//更新
            System.out.println("UPDATE：" + updateResponse.getResult());
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {//删除
            System.out.println("DELETE：" + updateResponse.getResult());
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {//没有操作
            System.out.println("NOOP：" + updateResponse.getResult());
        } else {
            System.out.println(updateResponse.getResult());
        }

        //异步执行
//        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
//            public void onResponse(UpdateResponse updateResponse) {
//                System.out.println(updateResponse.getIndex());
//                System.out.println(updateResponse.getType());
//                System.out.println(updateResponse.getId());
//                System.out.println(updateResponse.getVersion());
//                System.out.println(updateResponse.getResult());
//                System.out.println(updateResponse.getShardInfo());
//                System.out.println(updateResponse.getSeqNo());
//                System.out.println(updateResponse.getPrimaryTerm());
//                System.out.println(updateResponse.status());
//                System.out.println(updateResponse.toString());
//            }
//
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//        client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testDelete() throws IOException {
        //1.创建请求
        DeleteRequest deleteRequest = new DeleteRequest("test_post", "1");
        //2.执行
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        //3.处理结果
        System.out.println(deleteResponse.getIndex());
        System.out.println(deleteResponse.getType());
        System.out.println(deleteResponse.getId());
        System.out.println(deleteResponse.getVersion());
        System.out.println(deleteResponse.getSeqNo());
        System.out.println(deleteResponse.getPrimaryTerm());
        System.out.println(deleteResponse.getResult());

        //异步执行
//        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
//            public void onResponse(DeleteResponse deleteResponse) {
//                System.out.println(deleteResponse.getIndex());
//                System.out.println(deleteResponse.getType());
//                System.out.println(deleteResponse.getId());
//                System.out.println(deleteResponse.getVersion());
//                System.out.println(deleteResponse.getSeqNo());
//                System.out.println(deleteResponse.getPrimaryTerm());
//                System.out.println(deleteResponse.getResult());
//            }
//
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//        client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, listener);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void testBulk() throws IOException {
        //1.创建请求
        BulkRequest bulkRequest = new BulkRequest();
//        bulkRequest.add(new IndexRequest("post").id("1").source(XContentType.JSON, "filed", "1"));
//        bulkRequest.add(new IndexRequest("post").id("2").source(XContentType.JSON, "filed", "2"));
        bulkRequest.add(new UpdateRequest("post","1").doc(XContentType.JSON, "filed", "3"));
        bulkRequest.add(new DeleteRequest("post").id("2"));

        //2.执行
        //同步请求
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        //3.获取结果
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse response = bulkItemResponse.getResponse();
            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                    IndexResponse indexResponse = (IndexResponse) response;
                    System.out.println("INDEX：" + indexResponse.getResult());
                    break;
                case CREATE:
                    IndexResponse createResponse = (IndexResponse) response;
                    System.out.println("CREATE：" + createResponse.getResult());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) response;
                    System.out.println("UPDATE：" + updateResponse.getResult());
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) response;
                    System.out.println("DELETE：" + deleteResponse.getResult());
                    break;
                default:
                    System.out.println("其他！");
            }
        }

        //异步请求
//        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
//            public void onResponse(BulkResponse bulkItemResponses) {
//                //3.获取结果
//                for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
//                    DocWriteResponse response = bulkItemResponse.getResponse();
//                    switch (bulkItemResponse.getOpType()) {
//                        case INDEX:
//                            IndexResponse indexResponse = (IndexResponse) response;
//                            System.out.println("INDEX：" + indexResponse.getResult());
//                            break;
//                        case CREATE:
//                            IndexResponse createResponse = (IndexResponse) response;
//                            System.out.println("CREATE：" + createResponse.getResult());
//                            break;
//                        case UPDATE:
//                            UpdateResponse updateResponse = (UpdateResponse) response;
//                            System.out.println("UPDATE：" + updateResponse.getResult());
//                            break;
//                        case DELETE:
//                            DeleteResponse deleteResponse = (DeleteResponse) response;
//                            System.out.println("DELETE：" + deleteResponse.getResult());
//                            break;
//                        default:
//                            System.out.println("其他！");
//                    }
//                }
//            }
//
//            public void onFailure(Exception e) {
//
//            }
//        };
//        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
//        try {
//            Thread.sleep(5000);
//        } catch (
//                InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}