package ml.utils.hash;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

public class MurmurHashUtil {

    private static final String UTF_8 = "UTF-8";
    private static final long mask = 1L<<32;

    private static HashFunction murmur3 = Hashing.murmur3_128();
    private static HashFunction murmur3_32 = Hashing.murmur3_32();

    public static int hashInt(String input) {
        HashCode a = murmur3.hashString(input, Charset.forName(UTF_8));
        int hash = a.asInt();
        return hash;
    }

    public static Long hashLong(String input) {
        HashCode a = murmur3.hashString(input, Charset.forName(UTF_8));
        long hash = a.asLong();
        return hash;
    }

    /*
    * 截断hash
    * */
    public static int hashInt(String input, int dim) {
        HashCode a = murmur3_32.hashString(input, Charset.forName(UTF_8));
        long hash = a.asInt();
        if(hash < 0){
            hash += mask;
        }
        return (int)(hash & (dim-1));
    }

    public static Long hashLong(String input, long dim) {
        HashCode a = murmur3.hashString(input, Charset.forName(UTF_8));
        long hash = a.asLong();
        long mask = 1L << 64;
        if (hash < 0) {
            hash += mask;
        }
        return (hash % dim);
    }

    public static void main(String[] args) {

        String in = "user_id_2231195602";
        int dim = 1<<31;
        System.out.println(hashInt("350100", 500));
    }

//    public static void main(String[] args) {
//
//        String in = "murmurhash_poi_id_33323";
//        int dim = 31;
//        int hash = hashInt(in, dim);
//        System.out.println(hash);
//    }



}
