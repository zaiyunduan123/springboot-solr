package jesper_search.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jesper_search.bean.Hotel;

@Service
public class SolrService {

    private Logger logger = LoggerFactory.getLogger(SolrService.class);

    @Value("${solr.SOLR_URL}")
    public String SOLR_URL;

    private static SolrClient solrClient;

    private HttpSolrClient connetHttpSolrClientServer() {
        HttpSolrClient server = new HttpSolrClient(SOLR_URL);
        server.setSoTimeout(5000);
        server.setConnectionTimeout(1000);
        server.setDefaultMaxConnectionsPerHost(1000);
        server.setMaxTotalConnections(5000);
        return server;
    }


    /**
     * 按条件查询搜索引擎
     */

    public ArrayList<Hotel> querySolrIndex(String coreName, String query) {

        ArrayList<Hotel> hotels = null;
        try {
            solrClient = connetHttpSolrClientServer();
            QueryResponse rsp = null;
//            SolrQuery queryStr = new SolrQuery("*:*");
//            queryStr.addFilterQuery(query);
            SolrQuery queryStr = new SolrQuery();
            queryStr.setQuery("name:" + query); //搜索关键词
            rsp = solrClient.query(coreName, queryStr);
            hotels = documentList2Hotels(rsp.getResults());
            System.out.println(hotels);
        } catch (IOException | SolrServerException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            try {
                solrClient.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
        return hotels;
    }

    public ArrayList<Hotel> documentList2Hotels(SolrDocumentList list) {
        int length = list.size();
        ArrayList<Hotel> hotels = new ArrayList<Hotel>(length);
        if (length != 0) {
            Iterator<SolrDocument> it = list.iterator();
            SolrDocument doc = null;
            Hotel hotel = null;
            while (it.hasNext()) {
                doc = it.next();
                hotel = document2Hotel(doc);
                hotels.add(hotel);
            }
        }
        return hotels;
    }

    private Hotel document2Hotel(SolrDocument doc) {
        Hotel hotel = new Hotel();
        hotel.setId((Integer) doc.get("id"));
        hotel.setName((String) doc.get("name"));
        return hotel;
    }
}