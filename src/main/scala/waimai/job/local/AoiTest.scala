package waimai.job.local
import collection.JavaConverters._
import com.sankuai.waimai.aoi.core.index.WmAoiIndexBuilder

object AoiTest {

    def main(args: Array[String]): Unit = {
        val builder = new WmAoiIndexBuilder().buffer(0.0002)
        builder.index[Aoi]("POLYGON ((119.030249 33.598459, 119.030402 33.598523, 119.030684 33.598641, 119.031245 33.598706,119.031463 33.598377,119.031493 33.598249,119.032008 33.598303,119.032139 33.597814,119.031101 33.597579,119.030372 33.59742,119.030099 33.598459,119.030249 33.598459))", Aoi(1))
        builder.index[Aoi]("POLYGON ((117.342282 31.933459, 117.341557 31.931296, 117.34167  31.931119, 117.345141 31.93019, 117.345988 31.932476, 117.342282 31.933459))", Aoi(2))
        val reader = builder.build
        val result = reader.searchAoi(119.031068, 33.598067)
        println(result)
    }
}
