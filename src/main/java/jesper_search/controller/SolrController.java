package jesper_search.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import jesper_search.bean.Hotel;
import jesper_search.bean.SearchResult;
import jesper_search.service.SolrService;

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
}
