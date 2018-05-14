package jesper_search.service;

import java.io.IOException;
import java.util.*;

import jesper_search.mapper.HotelMapper;
import org.apache.juli.logging.Log;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import jesper_search.bean.Hotel;

@Service
public class SolrService {


    private Logger logger = LoggerFactory.getLogger(SolrService.class);

    @Value("${solr.SOLR_URL}")
    public String SOLR_URL;

    @Autowired
    private HotelMapper hotelMapper;


    private String solrEntry = "dvb_hotel";

    private String coreName = "ykz_hotel_search";

    public static SolrClient solrClient;

    public HttpSolrClient connetHttpSolrClientServer() {
        HttpSolrClient server = new HttpSolrClient(SOLR_URL);
        server.setSoTimeout(5000);
        server.setConnectionTimeout(1000);
        server.setDefaultMaxConnectionsPerHost(1000);
        server.setMaxTotalConnections(5000);
        return server;
    }

    /**
     * 定时全量索引
     */
    @Scheduled(cron = "*/3 * * * * ?")
    public void SyncFullData() {
        Map<String, String> map = new HashMap<>();
        //构建全量导入的入参:
        map.put("command", "full-import");
        map.put("clean", "true");
        map.put("commit", "true");
        map.put("optimize", "false");
        map.put("debug", "false");
        map.put("entity", solrEntry);

        SolrRequest<QueryResponse> request = new QueryRequest(new MapSolrParams(map));
        request.setPath("/dataimport");

        try {
            SolrClient solrClient = connetHttpSolrClientServer();
            NamedList<Object> response = solrClient.request(request, coreName);
            logger.info(new Date() + "vproduct 全量重建索引成功，" + response.toString());
        } catch (Exception e) {
            logger.info(new Date() + "vproduct 全量重建索引失败，异常信息：" + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 增量索引
     *
     * @param id
     */
    public void update(String id) {
        logger.info("更新索引  传入id是" + id);
        Hotel bean = hotelMapper.selectByPrimaryKey(Integer.parseInt(id));
        UpdateResponse resp = null;
        try {
            if (bean != null) {
                resp = solrClient.addBean("vproduct", bean, 60000);
                if (resp.getStatus() != 0) {
                    logger.error("实时更新索引 失败：List=" + bean + "，详细信息："
                            + resp.toString());
                } else {
                    logger.info("实时更新索引 成功：List=" + bean + "，详细信息："
                            + resp.toString());
                }
            } else {
                resp = solrClient.deleteById("ykz_hotel_search", id, 60000);
                if (resp.getStatus() != 0) {
                    logger.error("实时删除索引 失败：List=" + id + "，详细信息："
                            + resp.toString());
                } else {
                    logger.info("实时删除索引 成功：List=" + id + "，详细信息："
                            + resp.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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