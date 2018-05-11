package jesper_search;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import jesper_search.bean.Hotel;

import java.util.ArrayList;
import java.util.List;


public class SolrMain {

    public static String SOLR_URL;
    public static String SOLR_CORE;

    static {
        try {
            SOLR_URL = "http://localhost:8090/solr";
            SOLR_CORE = "ykz_hotel_search";
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        // 4.查询
        getDocument();
    }

    private static HttpSolrClient getSolrClient() {
        HttpSolrClient hsc = new HttpSolrClient(SOLR_URL);
        return hsc;
    }

    private static void getDocument() throws Exception {
        HttpSolrClient solrClient = getSolrClient();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("name:" + "七天"); //搜索关键词
        QueryResponse response = solrClient.query(SOLR_CORE, solrQuery);
        SolrDocumentList solrDocuments = response.getResults();
        long numFound = solrDocuments.getNumFound();
        List<Hotel> hotels = new ArrayList<>((int) numFound);
    }
}