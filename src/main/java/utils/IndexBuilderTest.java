package utils;

import com.sankuai.waimai.aoi.core.index.AoiArea;
import com.sankuai.waimai.aoi.core.index.WmAoiIndexBuilder;
import org.junit.Assert;
import org.junit.Test;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class IndexBuilderTest {

    //自定义加载aoi数据的使用方法
    @Test
    public void test() throws Exception {
        //1、初始化builder
        WmAoiIndexBuilder builder = new WmAoiIndexBuilder()
                .buffer(0.0002);

        //2、构建索引
        List<Pair<Integer, String>> list = new ArrayList<>();
        list.add(new Pair<>(1, "POLYGON ((119.030249 33.598459, 119.030402 33.598523, 119.030684 33.598641, 119.031245 33.598706,119.031463 33.598377,119.031493 33.598249,119.032008 33.598303,119.032139 33.597814,119.031101 33.597579,119.030372 33.59742,119.030099 33.598459,119.030249 33.598459))"));
        list.add(new Pair<>(2, "POLYGON ((117.342282 31.933459, 117.341557 31.931296, 117.34167  31.931119, 117.345141 31.93019, 117.345988 31.932476, 117.342282 31.933459))"));
        for (Pair<Integer, String> pair : list) {
            AoiArea aoiArea = new AoiArea();
            aoiArea.setId(pair.getKey());
            try {
                builder.index(pair.getValue(), aoiArea);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        WmAoiIndexBuilder.WmAoiIndex reader = builder.build();

        //3、数据查询
        Assert.assertEquals(1, reader.searchAoi(119.031068, 33.598067).getId());
        Assert.assertEquals(2, reader.searchAoi(117.343753, 31.931834).getId());
        Assert.assertNull(reader.searchAoi(118.794867, 31.931834));
    }

    @Test
    public void testWithSubClass() throws Exception {
        //1、初始化builder
        WmAoiIndexBuilder builder = new WmAoiIndexBuilder();

        //2、构建索引
        List<Pair<Integer, String>> list = new ArrayList<>();
        list.add(new Pair<>(1, "POLYGON((119.030249 33.598459,119.030402 33.598523,119.030684 33.598641,119.031245 33.598706,119.031463 33.598377,119.031493 33.598249,119.032008 33.598303,119.032139 33.597814,119.031101 33.597579,119.030372 33.59742,119.030099 33.598459,119.030249 33.598459))"));
        list.add(new Pair<>(2, "POLYGON ((117.342282 31.933459, 117.341557 31.931296, 117.34167 31.931119, 117.345141 31.93019, 117.345988 31.932476, 117.342282 31.933459))"));
        for (Pair<Integer, String> pair : list) {
            SubAoiArea subAoiArea = new SubAoiArea();
            subAoiArea.setId(pair.getKey());
            subAoiArea.setSubProperty("test sub property " + subAoiArea.getId());
            try {
                builder.index(pair.getValue(), subAoiArea);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        WmAoiIndexBuilder.WmAoiIndex reader = builder.build();

        //3、数据查询
        SubAoiArea subAoiArea = reader.searchAoi(119.031068, 33.598067, SubAoiArea.class);
        Assert.assertEquals(1, subAoiArea.getId());
        Assert.assertEquals(2, reader.searchAoi(117.343753, 31.931834, SubAoiArea.class).getId());
        Assert.assertNull(reader.searchAoi(118.794867, 31.931834));
    }

    private static class SubAoiArea extends AoiArea {
        private String subProperty;

        public String getSubProperty() {
            return subProperty;
        }

        public void setSubProperty(String subProperty) {
            this.subProperty = subProperty;
        }
    }


}
