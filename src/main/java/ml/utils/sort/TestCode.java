package ml.utils.sort;

import java.util.*;

/**
 * author: renjian
 * Datetime: 2021/5/11 下午4:26
 * email: renjian04@meituan.com
 * Description:
 */
public class TestCode {


    /**
     * 最大子序列的和
     * 数组里找重复数
     * 针对一个数组，问组合为某个数字sum的方法数
     * 最大连续子序列的和
     *
     * 广告流量的分配问题，是如何解决，有什么新的思路？
     * 给定一个场景如：在不同的渠道、广告的预算和消耗时长的约束下，如何构造目标函数？
     */

    public int maxSubArray(int[] nums) {
        int n = nums.length;
        int rs = Integer.MIN_VALUE;
        for (int i=0; i<n; i++) {
            int v = Math.max(rs+nums[i], nums[i]);
            rs = Math.max(v, rs);
        }
        return rs;
    }


    // 输入：s = "ADOBECODEBANC", t = "ABC"
    // 输出："BANC"
    public String minWindow(String s, String t) {
        Map<Character, Integer> tMap = new HashMap<>();
        Map<Character, Integer> cnt = new HashMap<>();
        for (int i=0; i<t.length(); i++) {
            char tv = t.charAt(i);
            tMap.put(tv, tMap.getOrDefault(tv, 0) + 1);
        }

        int leftRs = -1, rightRs = -1, leftPos = 0, rightPos = 0, sLen = s.length(), l = Integer.MAX_VALUE;
        while (rightPos < sLen) {
            char sv = s.charAt(rightPos);
            if (rightPos < sLen && tMap.containsKey(sv)) {
                cnt.put(sv, cnt.getOrDefault(sv, 0)+1);
            }
            while (check(tMap, cnt) && leftPos<=rightPos) {
                if (rightPos-leftPos+1<l) {
                    l = rightPos-leftPos+1;
                    leftRs = leftPos;
                    rightRs = rightPos+1;
                }
                char st = s.charAt(leftPos);
                if (cnt.containsKey(st)) {
                    cnt.put(st, cnt.get(st)-1);
                }
                leftPos++;
            }
            rightPos++;
        }
        return leftRs == -1 ? "" : s.substring(leftRs, rightRs);
    }


    public boolean check(Map<Character, Integer> tMap, Map<Character, Integer> cnt) {
        for (Map.Entry<Character, Integer> entry: tMap.entrySet()) {
            char v = entry.getKey();
            int c = entry.getValue();
            if (cnt.getOrDefault(v, 0) < c) return false;
        }
        return true;
    }


    public int minDistance(String word1, String word2) {

        char[] arr1 = word1.toCharArray(), arr2 = word2.toCharArray();
        int m = arr1.length, n = arr2.length;
        if (m == 0) return n;
        if (n == 0) return m;

        int[][] dp = new int[m+1][n+1];
        for (int i=1; i<n+1; i++) {
            dp[0][i] = i;
        }
        for (int i=1; i<m+1; i++) {
            dp[i][0] = i;
        }
        for (int i=1; i<m+1; i++) {
            for (int j=1; j<n+1; j++) {
                int v = arr1[i-1] == arr2[j-1] ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i-1][j]+1, dp[i][j-1]+1), dp[i-1][j-1]+v);
            }
        }
        return dp[m][n];

    }


    public List<List<Integer>> permute(int[] nums) {
        int n = nums.length;
        List<List<Integer>> rs = new ArrayList<>();
        if (n == 0) return rs;

        List<Integer> list = new ArrayList<>();
        for (int i=0; i<n; i++) {
            list.add(nums[i]);
        }
        dfs(rs, list, 0);
        return rs;
    }

    public void dfs(List<List<Integer>> rs, List<Integer> list, int idx) {
        int n = list.size();
        if (idx >= n) {
            rs.add(new ArrayList<>(list));
            return;
        }

        for (int i=idx; i<n; i++) {
            Collections.swap(list, idx, i);
            dfs(rs, list, idx+1);
            Collections.swap(list, i, idx);
        }
    }



    public int getLen(String str) {

        char[] arr = str.toCharArray();
        int n = arr.length;

        Set<Character> set = new HashSet<>();
        int rs = 0, pos=0;
        for (int i=0; i<n; i++) {
            int c = 0;
            if (i > 0) set.remove(arr[i-1]);
            for (int j=pos; j<n; j++) {
                char v = arr[j];
                if (set.contains(v)) {
                    rs = Math.max(rs, c);
                } else {
                    set.add(v);
                    c++;
                    pos = j;
                }
            }
            if (pos == n-1) return Math.max(rs, c);
        }
        return rs;
    }


    public int maxProduct(int[] nums) {
        int n = nums.length;

        int rs = nums[0], max = nums[0], min = nums[0];
        for (int i=1; i<n; i++) {
            int v = nums[i];
            max = Math.max(Math.max(max*v, min*v), v);
            min = Math.min(Math.min(max*v, min*v), v);
            rs = Math.max(rs, max);
//            System.out.println(max + ":"+min);
        }
        return rs;
    }


    /**
     * 由于字符串除了数字与括号外，只有加号和减号两种运算符。因此，如果展开表达式中所有的括号，则得到的新表达式中，数字本身不会发生变化，只是每个数字前面的符号会发生变化。
     *
     * 因此，我们考虑使用一个取值为 \{-1,+1\}{−1,+1} 的整数 \textit{sign}sign 代表「当前」的符号。根据括号表达式的性质，它的取值：
     *
     * 与字符串中当前位置的运算符有关；
     * 如果当前位置处于一系列括号之内，则也与这些括号前面的运算符有关：每当遇到一个以 -− 号开头的括号，则意味着此后的符号都要被「翻转」。
     * 考虑到第二点，我们需要维护一个栈 \textit{ops}ops，其中栈顶元素记录了当前位置所处的每个括号所「共同形成」的符号。例如，对于字符串 \text{1+2+(3-(4+5))}1+2+(3-(4+5))：
     *
     * 扫描到 \text{1+2}1+2 时，由于当前位置没有被任何括号所包含，则栈顶元素为初始值 +1+1；
     * 扫描到 \text{1+2+(3}1+2+(3 时，当前位置被一个括号所包含，该括号前面的符号为 ++ 号，因此栈顶元素依然 +1+1；
     * 扫描到 \text{1+2+(3-(4}1+2+(3-(4 时，当前位置被两个括号所包含，分别对应着 ++ 号和 -− 号，由于 ++ 号和 -− 号合并的结果为 -− 号，因此栈顶元素变为 -1−1。
     * 在得到栈 \textit{ops}ops 之后， \textit{sign}sign 的取值就能够确定了：如果当前遇到了 ++ 号，则更新 \textit{sign} \leftarrow \text{ops.top()}sign←ops.top()；如果遇到了遇到了 -− 号，则更新 \textit{sign} \leftarrow -\text{ops.top()}sign←−ops.top()。
     *
     * 然后，每当遇到 (( 时，都要将当前的 \textit{sign}sign 取值压入栈中；每当遇到 )) 时，都从栈中弹出一个元素。这样，我们能够在扫描字符串的时候，即时地更新 \textit{ops}ops 中的元素。
     *
     */
    public int calculate(String s) {
        Deque<Integer> ops = new LinkedList<Integer>();
        ops.push(1);
        int sign = 1;

        int ret = 0;
        int n = s.length();
        int i = 0;
        while (i < n) {
            if (s.charAt(i) == ' ') {
                i++;
            } else if (s.charAt(i) == '+') {
                sign = ops.peek();
                i++;
            } else if (s.charAt(i) == '-') {
                sign = -ops.peek();
                i++;
            } else if (s.charAt(i) == '(') {
                ops.push(sign);
                i++;
            } else if (s.charAt(i) == ')') {
                ops.pop();
                i++;
            } else {
                long num = 0;
                while (i < n && Character.isDigit(s.charAt(i))) {
                    num = num * 10 + s.charAt(i) - '0';
                    i++;
                }
                ret += sign * num;
            }
        }
        return ret;
    }


    /**
     * [1,2,1,3,2,5]
     * @param nums
     * @return
     */
    public int[] singleNumber(int[] nums) {
        int ret = 0;
        for (int n : nums) {
            ret ^= n;
        }
        int div = 1;
        while ((div & ret) == 0) {
            div <<= 1;
        }
        int a = 0, b = 0;
        for (int n : nums) {
            if ((div & n) != 0) {
                a ^= n;
            } else {
                b ^= n;
            }
        }
        return new int[]{a, b};
    }


    public int longestSubstring(String s, int k) {
        int n = s.length();
        Map<Character, Integer> map = new HashMap<>();
        for (int i=0; i<n; i++) {
            if (map.containsKey(s.charAt(i))) {
                map.put(s.charAt(i), map.get(s.charAt(i))+1);
            } else {
                map.put(s.charAt(i), 1);
            }
        }
        int rs = n;
        if (judge(map, k)) return rs;
        int i=0, j=n-1;
        while (i<=j) {
            while (i<=j && map.get(s.charAt(i)) < k) {
                map.put(s.charAt(i), map.get(s.charAt(i))-1);
                i++;
                rs--;
            }
            while (i<=j && map.get(s.charAt(j)) < k) {
                map.put(s.charAt(j), map.get(s.charAt(j))-1);
                j--;
                rs--;
            }
            if (judge(map, k)) return rs;
            if (map.get(s.charAt(i)) <= map.get(s.charAt(j))) {
                map.put(s.charAt(i), map.get(s.charAt(i))-1);
                i++;
                rs--;
            } else {
                map.put(s.charAt(j), map.get(s.charAt(j))-1);
                j--;
                rs--;
            }
        }
        return 0;
    }

    public boolean judge(Map<Character, Integer> map, int k) {
        for (Map.Entry<Character, Integer> entry: map.entrySet()) {
            if (entry.getValue() < k) return false;
        }
        return true;
    }

    /**
     * [7,8,9,11,12]
     * [2,3,4,1,2]
     * @param nums
     * @return
     */
    public int firstMissingPositive(int[] nums) {
        int n = nums.length;

        for (int i=0; i<n; i++) {
            if (nums[i] < 0) nums[i] = 0;
        }

        for (int i=0; i<n; i++) {
            if (nums[i] >= 1 && nums[i] <= n) {
                int v = nums[i] - 1;
                nums[v] = -1;
            }
        }

        for (int i=0; i<n; i++) {
            if (nums[i] >= 0) return i+1;
        }
        return n+1;
    }



    public String getPermutation(int n, int k) {
        int[] vs = new int[n+1];
        vs[0] = 1;
        List<Integer> pq = new LinkedList<>();
        for (int i=1; i<=n; i++) {
            vs[i] = vs[i-1] * i;
            pq.add(i);
        }

        StringBuffer sb = new StringBuffer();
        int idx = n-1;
        while (k >= 0 && idx >= 0) {
            for (int i=1; i<=idx; i++) {
                if (vs[idx] * i >= k) {
                    int v = pq.get(i-1);
                    pq.remove(i-1);
                    sb.append(v);
                    k -= vs[idx] * (i - 1);
                    break;
                }
            }
            idx--;
        }
        if (!pq.isEmpty()) sb.append(pq.get(0));
        return sb.toString();
    }


//
//    public int maxProfit(int[] prices) {
//
//    }
//
//
//
//    public int maximalRectangle(char[][] matrix) {
//
//    }

}



