package jesper.search.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import jesper.search.bean.Hotel;
import jesper.search.bean.SearchResult;
import jesper.search.service.SolrService;

import java.io.IOException;
import java.util.ArrayList;


@Controller
public class SolrController {

    @Autowired
    private SolrService solrService;

    private String coreName = "ykz_hotel_search";

    /**
     * 按条件查询搜索引擎
     */
    @ResponseBody
    @ApiOperation(value = "搜索接口", notes = "搜索接口详细描述")
    @RequestMapping(value = "/search", method = RequestMethod.GET)
    public SearchResult querySolrIndex(@ApiParam(required = true, name = "name", value = "酒店名称")
                                       @RequestParam(name = "name", required = true) String name) {
        SearchResult result = null;
        ArrayList<Hotel> hotels = solrService.querySolrIndex(coreName, name);
        result = new SearchResult();
        result.setData(hotels);
        result.setTotal(hotels.size());

        return result;
    }

    /**
     * 按条件查询搜索引擎
     */
    @ResponseBody
    @ApiOperation(value = "范围搜索接口", notes = "根据经纬度搜索附近200公里的酒店")
    @RequestMapping(value = "/rangeSearch", method = RequestMethod.GET)
    public SearchResult rangeSearch(@ApiParam(required = true, name = "lat", value = "经度")
                                    @RequestParam(name = "lat", required = true) String lat,
                                    @ApiParam(required = true, name = "lng", value = "维度")
                                    @RequestParam(name = "lng", required = true) String lng) {
        SearchResult result = null;
        ArrayList<Hotel> hotels = null;
        try {
            hotels = solrService.rangeSearch(lat, lng);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
        result = new SearchResult();
        result.setData(hotels);
        result.setTotal(hotels.size());
        return result;
    }
}
