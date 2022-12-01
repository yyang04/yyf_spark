package ml.utils.hash;

import java.security.MessageDigest;

/**
 * Created by xieqianlong on 2017/2/15.
 */
public class Md5SignUtil {
    public static String Md5Sign(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(input.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                //注意&之后可能出现高位为0
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }

    public static Integer Sign28(String input) {
        try {
            String md5 = Md5Sign(input);
            if (null != md5) {
                //4*7正好28位
                String subStr = md5.substring(0, 7);
                return Integer.parseInt(subStr, 16);
            }
        }
        catch (Exception e) {

        }
        return null;
    }

    //N<28
    public static Integer SignN(String input, int N) {
        int mask = (1<<N)-1;
        try {
            String md5 = Md5Sign(input);
            if (null != md5) {
                String subStr = md5.substring(0, 7);
                return Integer.parseInt(subStr, 16) & mask;
            }
        }
        catch (Exception e) {
            System.out.printf(e.toString());
        }
        return null;
    }

    //mask<268435455
    public static Integer SignMask(String input, int mask) {
        try {
            String md5 = Md5Sign(input);
            if (null != md5) {
                String subStr = md5.substring(0, 7);
                return Integer.parseInt(subStr, 16) & mask;
            }
        }
        catch (Exception e) {
            System.out.printf(e.toString());
        }
        return null;
    }


    public static Long SignMaskL(String input, int mask) {
        try {
            String md5 = Md5Sign(input);
//            System.out.println(md5);
            if (null != md5) {
                String subStr = md5.substring(0, 7);
                return Long.parseLong(subStr, 16) & mask;
            }
        }
        catch (Exception e) {
            System.out.printf(e.toString());
        }
        return null;
    }



}
