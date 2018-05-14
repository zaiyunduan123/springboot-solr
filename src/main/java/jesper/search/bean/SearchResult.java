package jesper.search.bean;

import lombok.Data;

import java.util.List;

/**
 * Created by jiangyunxiong on 2018/5/10.
 */
@Data
public class SearchResult<T> {

    private int total;

    private List<T> data;
}
