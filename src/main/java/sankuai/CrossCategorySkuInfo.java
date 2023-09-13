package sankuai;

import java.io.Serializable;

public class CrossCategorySkuInfo implements Serializable {
    private long id;

    private long spuId;

    private long firstCategoryId;

    private long secondCategoryId;

    private long thirdCategoryId;

    public CrossCategorySkuInfo() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSpuId() {
        return spuId;
    }

    public void setSpuId(long spuId) {
        this.spuId = spuId;
    }

    public long getFirstCategoryId() {
        return firstCategoryId;
    }

    public void setFirstCategoryId(long firstCategoryId) {
        this.firstCategoryId = firstCategoryId;
    }

    public long getSecondCategoryId() {
        return secondCategoryId;
    }

    public void setSecondCategoryId(long secondCategoryId) {
        this.secondCategoryId = secondCategoryId;
    }

    public long getThirdCategoryId() {
        return thirdCategoryId;
    }

    public void setThirdCategoryId(long thirdCategoryId) {
        this.thirdCategoryId = thirdCategoryId;
    }
}