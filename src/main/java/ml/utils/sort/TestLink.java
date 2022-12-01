package ml.utils.sort;

import java.util.LinkedList;
import java.util.List;

/**
 * author: renjian
 * Datetime: 2021/1/4 下午11:14
 * email: renjian04@meituan.com
 * Description:
 */
public class TestLink {


    public static void main(String[] args) {

        List<Integer> rs = new LinkedList<>();

        for (int i = 0; i < 10000; i++) {
            rs.add(i);
        }


        for (Integer r : rs) {
            if (r % 10 == 0) {
                rs.remove(r);
            }
        }
    }
}
