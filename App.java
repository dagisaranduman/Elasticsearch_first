package com.bigdatacompany.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class App {

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch").build();
        //Add transport addresses and do something with the client...

        /**
         * The TransportClient connects remotely to an Elasticsearch cluster using the transport module.
         * It does not join the cluster, but simply gets one or
         * more initial transport addresses and communicates with them in round robin fashion on each action
         * (though most actions will probably be "two hop" operations).
         */
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        /*  List<DiscoveryNode> discoveryNodes = client.listedNodes();//node'ları listele for döngüsü yardımıyla

        for(int i=0; i<discoveryNodes.size(); i++){
            System.out.println(discoveryNodes.get(i));
        }

        for(DiscoveryNode node : discoveryNodes){

            System.out.println(node);
        }
         */

        /**
         * oluşturduğumuz JSON verimizi elasticsearch'e indexlemiş olduk
         */
        /*Map<String, Object> json = new HashMap<String, Object>();
        json.put("name","Apple Macbook Air");
        json.put("detail","Intel Core i5, 16G Ram, 128GB SSD");
        json.put("price","5420 TL");
        json.put("provider","Apple Turkey");

        //Index API = veri ekleme olayı
        IndexResponse indexResponse = client.prepareIndex("product", "doc", "3")
                .setSource(json, XContentType.JSON)
                .get();
        System.out.println(indexResponse.getId());
         */

       /*
        //GET API = bunun sayesinde elasticsearch'deki verileri javaya alabiliyoruz
        GetResponse response = client.prepareGet("product", "doc", "3").get();

        Map<String, Object> source = response.getSource();

       String name= (String) source.get("name");
       String price= (String) source.get("price");
       String detail= (String) source.get("detail");
       String provider= (String) source.get("provider");

        System.out.println("name = "+name);
        System.out.println("price = "+price);
        System.out.println("detail = "+detail);
        System.out.println("provider = "+provider);
        */

        /*
        //search API = bunun sayesinde id bilmeden elasticsearch'deki verileri javaya alabiliyoruz
        SearchResponse response = client.prepareSearch("product")
                .setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("provider", "Turkey"))
                .get();

       //sorgunun sonuçlarını barındıran objectlerin array'i
        //tÃ¼m sonuÃ§larÄ± burada aldÄ±
        SearchHit[] hits = response.getHits().getHits();

        for(SearchHit hit : hits){
            //burada da her bir sonucu verdi
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        */

        /*
        //Delete API = silmek için
        DeleteResponse deleteResponse = client.prepareDelete("product", "_doc", "1").get();

        System.out.println(deleteResponse.getId());
        */

        //Delete API = id olmadan silmek için
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("name", "Apple"))
                        .source("product")
                        .get();
        System.out.println(response.getDeleted());



    }

}
