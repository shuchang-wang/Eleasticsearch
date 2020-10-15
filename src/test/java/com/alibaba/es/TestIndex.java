package com.alibaba.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {
    @Autowired
    private RestHighLevelClient client;

    /**
     * 同步创建索引
     *
     * @throws IOException
     */
    @Test
    public void testCreateIndex() throws IOException {
        /*    PUT /my_index
    {
        "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
        "mappings": {
        "properties": {
            "filed1": {
                "type": "text"
            },
            "filed2": {
                "type": "text"
            }
        }
    },
        "aliases": {
        "default_index": {}
    }
    }*/
        //1.获取请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");

        //设置请求参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "1").build());

        //设置映射1
        createIndexRequest.mapping("{\n" +
                "        \"properties\": {\n" +
                "            \"filed1\": {\n" +
                "                \"type\": \"text\"\n" +
                "            },\n" +
                "            \"filed2\": {\n" +
                "                \"type\": \"text\"\n" +
                "            }\n" +
                "        }\n" +
                "    }", XContentType.JSON);
        //设置映射2
//        Map<String, Object> filed1 = new HashMap<String, Object>();
//        filed1.put("type", "text");
//        filed1.put("analyzer", "standard");
//        Map<String, Object> filed2 = new HashMap<String, Object>();
//        filed2.put("type", "text");
//        Map<String, Object> properties = new HashMap<String, Object>();
//        properties.put("filed1", filed1);
//        properties.put("filed2", filed2);
//        Map<String, Object> mapping = new HashMap<String, Object>();
//        mapping.put("properties", properties);
//        createIndexRequest.mapping(mapping);
        //设置映射3
//        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
//        xContentBuilder.startObject();
//        {
//            xContentBuilder.startObject("fild1");
//            {
//                xContentBuilder.field("type","text");
//            }
//            xContentBuilder.endObject();
//            xContentBuilder.startObject("fild2");
//            {
//                xContentBuilder.field("type","text");
//            }
//            xContentBuilder.endObject();
//        }
//        xContentBuilder.endObject();
//        createIndexRequest.mapping(xContentBuilder);
        //设置别名
        createIndexRequest.alias(new Alias("prod_index"));

        //==================可选参数==================
        //设置超时时间
        createIndexRequest.setTimeout(TimeValue.timeValueSeconds(5));
        //主节点超时时间
        createIndexRequest.setMasterTimeout(TimeValue.timeValueSeconds(5));
        //设置创建索引API返回响应之前等待活动分片的数量
        createIndexRequest.waitForActiveShards(ActiveShardCount.from(1));

        //2.执行
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        //3.处理结果
        //索引
        String index = createIndexResponse.index();
        System.out.println("index：" + index);
        //得到响应（全部）
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("acknowledged：" + acknowledged);
        //得到响应 指示是否在超时前为索引中的每个分片启动了所需数量的碎片副本
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("shardsAcknowledged：" + shardsAcknowledged);

    }

    /**
     * 异步创建索引
     *
     * @throws IOException
     */
    @Test
    public void testCreateIndexAsync() throws IOException {
        //1.获取请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index");

        //设置请求参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "1").build());

        //设置映射1
        createIndexRequest.mapping("{\n" +
                "        \"properties\": {\n" +
                "            \"filed1\": {\n" +
                "                \"type\": \"text\"\n" +
                "            },\n" +
                "            \"filed2\": {\n" +
                "                \"type\": \"text\"\n" +
                "            }\n" +
                "        }\n" +
                "    }", XContentType.JSON);
        //设置别名
        createIndexRequest.alias(new Alias("prod_index"));

        //==================可选参数==================
        //设置超时时间
        createIndexRequest.setTimeout(TimeValue.timeValueSeconds(5));
        //主节点超时时间
        createIndexRequest.setMasterTimeout(TimeValue.timeValueSeconds(5));
        //设置创建索引API返回响应之前等待活动分片的数量
        createIndexRequest.waitForActiveShards(ActiveShardCount.from(1));

        //2.执行
        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                //3.处理结果
                //索引
                String index = createIndexResponse.index();
                System.out.println("index：" + index);
                //得到响应（全部）
                boolean acknowledged = createIndexResponse.isAcknowledged();
                System.out.println("acknowledged：" + acknowledged);
                //得到响应 指示是否在超时前为索引中的每个分片启动了所需数量的碎片副本
                boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
                System.out.println("shardsAcknowledged：" + shardsAcknowledged);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.indices().createAsync(createIndexRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步删除索引
     *
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        //构建请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("my_index");
        deleteIndexRequest.timeout(TimeValue.timeValueSeconds(5));
        deleteIndexRequest.masterNodeTimeout(TimeValue.timeValueSeconds(5));

        //同步执行
        AcknowledgedResponse acknowledgedResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //处理响应
        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    /**
     * 异步删除索引
     *
     * @throws IOException
     */
    @Test
    public void testDeleteIndexAsync() throws IOException {
        //构建请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("my_index");
        deleteIndexRequest.timeout(TimeValue.timeValueSeconds(5));
        deleteIndexRequest.masterNodeTimeout(TimeValue.timeValueSeconds(5));

        //异步执行
        ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                //处理响应
                boolean acknowledged = acknowledgedResponse.isAcknowledged();
                System.out.println(acknowledged);
                System.out.println(acknowledgedResponse.toString());
                System.out.println("删除索引成功！！！");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("删除索引失败！！！");
                e.printStackTrace();
            }
        };
        client.indices().deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 同步获取索引 Index exists API
     *
     * @throws IOException
     */
    @Test
    public void testExistsIndex() throws IOException {
        //构建请求
        GetIndexRequest getIndexRequest = new GetIndexRequest("my_index");
        //=========================参数=========================
        //从主节点返回本地索引信息状态
        getIndexRequest.local(false);
        //以适合人类的格式返回
        getIndexRequest.humanReadable(true);
        //是否返回每个索引的所有默认配置
        getIndexRequest.includeDefaults(false);
        //执行
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //处理响应
        System.out.println(exists);
    }

    /**
     * 异步获取索引 Index exists API
     *
     * @throws IOException
     */
    @Test
    public void testExistsIndexAsync() throws IOException {
        //构建请求
        GetIndexRequest getIndexRequest = new GetIndexRequest("my_index");
        //=========================参数=========================
        //从主节点返回本地索引信息状态
        getIndexRequest.local(false);
        //以适合人类的格式返回
        getIndexRequest.humanReadable(true);
        //是否返回每个索引的所有默认配置
        getIndexRequest.includeDefaults(false);
        //执行
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {

            @Override
            public void onResponse(Boolean exists) {
                //处理响应
                System.out.println(exists);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.indices().existsAsync(getIndexRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步关闭索引
     */
    @Test
    public void testCloseIndex() throws IOException {
        //构建请求
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("my_index");
        //执行
        CloseIndexResponse closeIndexResponse = client.indices().close(closeIndexRequest, RequestOptions.DEFAULT);
        //处理响应
        boolean acknowledged = closeIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    /**
     * 异步关闭索引
     */
    @Test
    public void testCloseIndexAsync() {
        //构建请求
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("my_index");
        //执行
        ActionListener<CloseIndexResponse> listener = new ActionListener<CloseIndexResponse>() {
            @Override
            public void onResponse(CloseIndexResponse closeIndexResponse) {
                boolean acknowledged = closeIndexResponse.isAcknowledged();
                System.out.println(acknowledged);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.indices().closeAsync(closeIndexRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步打开索引
     *
     * @throws IOException
     */
    @Test
    public void testOpenIndex() throws IOException {
        //构建请求
        OpenIndexRequest openIndexRequest = new OpenIndexRequest("my_index");
        //执行
        OpenIndexResponse openIndexResponse = client.indices().open(openIndexRequest, RequestOptions.DEFAULT);
        //处理响应结果
        boolean acknowledged = openIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    /**
     * 异步打开索引
     *
     * @throws IOException
     */
    @Test
    public void testOpenIndexAsync() {
        //构建请求
        OpenIndexRequest openIndexRequest = new OpenIndexRequest("my_index");
        ActionListener<OpenIndexResponse> listener = new ActionListener<OpenIndexResponse>() {
            @Override
            public void onResponse(OpenIndexResponse openIndexResponse) {
                //处理响应结果
                boolean acknowledged = openIndexResponse.isAcknowledged();
                System.out.println(acknowledged);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        //执行
        client.indices().openAsync(openIndexRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
