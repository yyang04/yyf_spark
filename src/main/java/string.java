import java.util.HashMap;
import java.util.Map;

public class string {

    public static void main(String[] args) {
        Map<String, String> a = new HashMap<>();
        a.put("st", "1");

        int result = Integer.parseInt(a.get("s2"));
        System.out.println(result);
    }
}
