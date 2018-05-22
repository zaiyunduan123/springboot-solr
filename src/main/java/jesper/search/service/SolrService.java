package jesper.search.service;

import java.io.IOException;
import java.util.*;

import jesper.search.mapper.HotelMapper;
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
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import jesper.search.bean.Hotel;

import javax.annotation.Resource;

@Service("SolrService")
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
            logger.info(new Date() + "ykz_hotel_search 全量重建索引成功，" + response.toString());
        } catch (Exception e) {
            logger.info(new Date() + "ykz_hotel_search 全量重建索引失败，异常信息：" + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 增量索引
     * @param id
     */
    public void update(String id) {
        logger.info("更新索引传入id是" + id);
        Hotel bean = hotelMapper.selectByPrimaryKey(Integer.parseInt(id));
        UpdateResponse resp = null;
        try {
            if (bean != null) {
                resp = solrClient.addBean(coreName, bean, 60000);
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
     * 根据经纬度搜索附近200公里的酒店
     *
     * @param cityLat
     * @param cityLng
     * @return
     * @throws IOException
     * @throws SolrServerException
     */
    public ArrayList<Hotel> rangeSearch(String cityLat, String cityLng) {
        Float lat = Float.parseFloat(cityLat);
        Float lng = Float.parseFloat(cityLng);


        ArrayList<Hotel> hotels = null;
        try {
            solrClient = connetHttpSolrClientServer();
            QueryResponse rsp = null;
            SolrQuery query = new SolrQuery();
            if (cityLat != null && cityLng != null) {
                // 使结果集约束在到中心点位置的最大距离（km）圆形区域内。
                query.addFilterQuery("{!geofilt}");        //距离过滤函数
                query.set("sfield", "product_pos_rpt"); //指定坐标索引字段
                query.set("pt", cityLat + "," + cityLng);//当前经纬度
                query.set("d", 200); //就近 d km的所有数据 //params.set("score", "kilometers");
                rsp = solrClient.query(coreName, query);
                hotels = documentList2Hotels(rsp.getResults());
                System.out.println(hotels);
            }else{
                /**
                 *  权重排序
                 *  权重设置的字段
                 *  1、价格，价格越低越排在前面
                 *  2、销量，销量越高越排在前面
                 *  3、好评度，好评度越高越排在前面
                 */

                String scoreMethod = "sum(linear(div(10000,price),1000,10),linear(count,1.3,3),linear(goodRank,1000,3))";
                query.set("defType", "edismax");//defType有两种，edismax支持boost函数与score相乘作为，而dismax只能使用bf作用效果是相加
                query.set("bf", scoreMethod);
            }
            rsp = solrClient.query(coreName, query);
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

    /**
     * 按条件查询搜索引擎
     */

    public ArrayList<Hotel> querySolrIndex(String query) {

        ArrayList<Hotel> hotels = null;
        try {
            solrClient = connetHttpSolrClientServer();
            QueryResponse rsp = null;
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

    /**
     * 模糊查询
     *
     * @return
     */
    public ArrayList<Hotel> likeSearch(String name) {
        SolrQuery query = new SolrQuery();
        ArrayList<Hotel> hotels = null;
        if (StringUtils.isEmpty(name)) {
            query.setQuery("*:*");  //组装查询条件
        } else if (name.length() == 1) {
            query.setQuery(name + "*");
        } else {
            //整词模糊匹配
            String searchWord = name;
            searchWord = searchWord.replaceAll("\\s+", " AND ");
            query.setQuery("searchText:" + searchWord + " OR name_ws:*" + searchWord + "*");
        }
        QueryResponse response = null;
        try {
            response = solrClient.query(coreName, query);
            hotels = documentList2Hotels(response.getResults());
        } catch (Exception e) {
            e.printStackTrace();
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