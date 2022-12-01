package ml.utils.hash;

/**
 * author: renjian
 * Datetime: 2021/1/4 下午11:14
 * email: renjian04@meituan.com
 * Description:
 */
public class LargeCode {

    private static int facet_bits = 10;

    public static Long makeSingleIdHash(Long origin_id, int facet_id, int mask_dim, boolean cache) {
        long result = 0;
        //facet
        int base_bit = 0;
        result |= facet_id;
        base_bit += facet_bits;

        if (cache) {
            result |= 1 << base_bit;
        }
        base_bit += 1;

        // features
        int mask = (1 << mask_dim) - 1;
        result |= (origin_id & mask) << base_bit;

        return result;

    }


    public static void main(String[] args) {

        int a = MurmurHashUtil.hashInt("poi_id_-1", 1<<31);
        System.out.printf(makeSingleIdHash(new Long(a), 738, 31, true).toString());
    }

}
