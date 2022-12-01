package ml.utils.sort;

import java.util.*;

/**
 * author: renjian
 * Datetime: 2021/1/4 下午11:14
 * email: renjian04@meituan.com
 * Description:
 */
public class ListSort {



    /**
     * 最小覆盖子串
     * @param s
     * @param t
     * @return
     */
    public String minWindow(String s, String t) {
        Map<Character, Integer> tMap = new HashMap<>();
        for (int i = 0; i < t.length(); i++) {
            tMap.put(t.charAt(i), tMap.getOrDefault(t.charAt(i), 0)+1);
        }
        int i=0, j=0, n=s.length();
        int minI = 0, minJ = n-1;
        Map<Character, Integer> sMap = new HashMap<>();
        while (j < n) {
            sMap.put(s.charAt(j), sMap.getOrDefault(s.charAt(j), 0)+1);
            if (minWindowJudge(sMap, tMap)) {
                minJ = j;
                break;
            }
            j++;
        }
        i++;
        j++;
        while (i < n) {
            char v =  s.charAt(i-1);
            if (tMap.containsKey(v)) {
                if (j == n) break;
                while (j < n) {
                    if (s.charAt(j) == v) {
                        if (j-i < minJ-minI) {
                            minI = i;
                            minJ = j;
                        }
                        break;
                    }
                    j++;
                }
            } else {
                minI = i;
                minJ = j;
            }
            System.out.println(minI);
            System.out.println(minJ);
            i++;
        }

        return s.substring(minI, minJ+1);
    }

    public boolean minWindowJudge(Map<Character, Integer> sMap, Map<Character, Integer> tMap) {
        for (Map.Entry<Character, Integer> entry : tMap.entrySet()) {
            if (sMap.getOrDefault(entry.getKey(), 0) < entry.getValue()) return false;
        }
        return true;
    }

    /**
     * 最长连续序列
     * @param nums
     * @return
     */
//    public int maxSeqLen(int[] nums) {
//
//    }

    /**
     * 排列序列
     * @param n
     * @param k
     * @return
     */
    public String getPermutation(int n, int k) {
        int d = 1;
        LinkedList<Integer> v = new LinkedList<>();
        for (int i = 1; i <= n; i++) {
            d *= i;
            v.add(i);
        }
        StringBuffer rs = new StringBuffer();
        getPermutationRecursive(n-1, d/n, k, v, rs);
        rs.append(v.get(0));
        return rs.toString();
    }

    public void getPermutationRecursive(int n, int d, int k, LinkedList<Integer> v, StringBuffer rs) {
        if (n == 0) return;
        int i = k / d;
        int j = k % d;
        if (j == 0) i--;
        rs.append(v.get(i));
        v.remove(i);
        getPermutationRecursive(n-1, d / n, j, v, rs);
    }


    /**
     * N皇后问题 -- 返回解决方案
     * @param n
     * @return
     */
    public List<List<String>> solveNQueens1(int n) {
        int[][] data = new int[n][n];
        List<List<String>> rs = new ArrayList<>();
        solveNQueensDfs(data, n, 0,  rs);
        return rs;
    }

    public void solveNQueensDfs(int[][] data, int n, int i, List<List<String>> rs) {
        if (i == n) {
            rs.add(nQueensTran(data));
        } else {
            for (int j = 0; j < n; j++) {
                if (!nQueensJudge(data, n, i, j)) {
                    continue;
                }
                data[i][j] = 1;
                solveNQueensDfs(data, n, i+1, rs);
                data[i][j] = 0;
            }
        }
    }

    public List<String> nQueensTran(int[][] data) {
        List<String> rs = new ArrayList<>();
        int n = data.length;
        for (int i = 0; i < n; i++) {
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j < n; j++) {
                if (data[i][j] == 0) sb.append('.');
                else sb.append('Q');
            }
            rs.add(sb.toString());
        }
        return rs;
    }

    /**
     * N皇后问题2 -- 返回解决方案数量
     * @param n
     * @return
     */
    public int totalNQueens(int n) {
        int[][] data = new int[n][n];
        return totalNQueensDfs(data, n, 0, 0, 0, 0);
    }

    public int totalNQueensDfs(int[][] data, int n, int i, int j, int count, int rs) {
        if (i == n) {
            if (count == n) rs += 1;
            return rs;
        }
        if (i < n && count == n) {
            rs += 1;
            return rs;
        }
        if (j == n-1) {
            i++;
            j = 0;
        } else {
            j++;
        }
        int v = 0;
        if (nQueensJudge(data, n, i, j)) {
            data[i][j] = 1;
            v = totalNQueensDfs(data, n, i, j, count+1, rs);
            data[i][j] = 0;
        }
        v = Math.max(v, totalNQueensDfs(data, n, i, j, count, rs));
        return v;
    }

    public boolean nQueensJudge(int[][] data, int n, int i, int j) {
        for (int k=0; k<n; k++) {
            if (data[i][k] == 1 || data[k][j] == 1) return false;
        }
        for (int k = 1; k < Math.min(i, j); k++) {
            if (data[i-k][j-k] == 1) return false;
        }
        return true;
    }

    /**
     * 缺失的第一个正整数
     * @param nums
     * @return
     */
    public int firstMissingPositive(int[] nums) {
        int n = nums.length;
        int rs = n+1;

        int i = 0;
        while (i < n) {
            if (nums[i] > n || nums[i] <= 0 || nums[i] == i+1) {
                i++;
                continue;
            }
            swap(nums, i, nums[i]-1);
        }
        for (int j=0; j<n; j++) {
            if (nums[j] != j+1) {
                return j+1;
            }
        }
        return rs;
    }

    /**
     *最大不连续子序列乘积
     * @param nums
     * @return
     */
    public double maxSubArray(double[] nums) {
        double rs = 0;
        int n = nums.length;
        if (n == 0) return rs;
        double maxV = nums[0], minV = nums[0];
        for (int i = 1; i < n; i++) {
            double v1 = Math.max(maxV * nums[i], minV * nums[i]);
            double v2 = Math.min(maxV * nums[i], minV * nums[i]);
            maxV = Math.max(Math.max(v1, nums[i]), maxV);
            minV = Math.min(Math.min(v2, nums[i]), minV);
        }
        return maxV;


    }

    /**
     * 解数独
     * @param board
     */
    public void solveSudoku(char[][] board) {
        int n = board[0].length;
        solveSudokuDfs(board, n, 0, 0);
    }
    public boolean solveSudokuDfs(char[][] board, int n, int i, int j) {
        if (i == n) return true;
        if (board[i][j] != '.') {
            return j + 1 == n ? solveSudokuDfs(board, n, i + 1, 0) : solveSudokuDfs(board, n, i, j + 1);
        } else {
            for (int k = 1; k <= n; k++) {
                char v = String.valueOf(k).charAt(0);
                if (!solveSudokuJudge(board, n, i, j, v)) continue;
                board[i][j] = v;
                boolean b = j + 1 == n ? solveSudokuDfs(board, n, i + 1, 0) : solveSudokuDfs(board, n, i, j + 1);
                if (b) return b;
                board[i][j] = '.';
            }
            return false;
        }
    }
    public boolean solveSudokuJudge(char[][] board, int n, int i, int j, char v) {
        for (int p = 0; p < n; p++) {
            if (board[i][p] == v) return false;
            if (board[p][j] == v) return false;
        }
        int p = i / 3, q = j / 3;
        for (int l = p * 3; l < (p + 1) * 3; l++) {
            for (int r = q * 3; r < (q + 1) * 3; r++) {
                if (board[l][r] == v) return false;
            }
        }
        return true;
    }

    /**
     * 全排列
     * @param nums
     * @return
     */
    public List<List<Integer>> permute(int[] nums) {
        List<List<Integer>> rs = new ArrayList<>();
        int n = nums.length;
        if (n == 0) return rs;
        permuteDfs(nums, rs, n, 0, new ArrayList<>());
        return rs;
    }
    public void permuteDfs(int[] nums, List<List<Integer>> rs, int n, int i, List<Integer> tmp) {

        if (i == n) {
            rs.add(new ArrayList<>(tmp));
        } else {
            for (int k = 0; k < n; k++) {
                if (tmp.contains(nums[k])) continue;
                tmp.add(nums[k]);
                permuteDfs(nums, rs, n, i+1, tmp);
//                System.out.println(nums[k]);
                tmp.remove(i);
            }
        }
    }



    /**
     * N皇后
     * @param n
     * @return
     */
    public List<List<String>> solveNQueens(int n) {
        List<List<String>> solutions = new ArrayList<List<String>>();
        int[] queens = new int[n];
        Arrays.fill(queens, -1);
        Set<Integer> columns = new HashSet<Integer>();
        Set<Integer> diagonals1 = new HashSet<Integer>();
        Set<Integer> diagonals2 = new HashSet<Integer>();
        backtrack(solutions, queens, n, 0, columns, diagonals1, diagonals2);
        return solutions;
    }

    public void backtrack(List<List<String>> solutions, int[] queens, int n, int row, Set<Integer> columns, Set<Integer> diagonals1, Set<Integer> diagonals2) {
        if (row == n) {
            List<String> board = generateBoard(queens, n);
            solutions.add(board);
        } else {
            for (int i = 0; i < n; i++) {
                if (columns.contains(i)) {
                    continue;
                }
                int diagonal1 = row - i;
                if (diagonals1.contains(diagonal1)) {
                    continue;
                }
                int diagonal2 = row + i;
                if (diagonals2.contains(diagonal2)) {
                    continue;
                }
                queens[row] = i;
                columns.add(i);
                diagonals1.add(diagonal1);
                diagonals2.add(diagonal2);
                backtrack(solutions, queens, n, row + 1, columns, diagonals1, diagonals2);
                queens[row] = -1;
                columns.remove(i);
                diagonals1.remove(diagonal1);
                diagonals2.remove(diagonal2);
            }
        }
    }

    public List<String> generateBoard(int[] queens, int n) {
        List<String> board = new ArrayList<String>();
        for (int i = 0; i < n; i++) {
            char[] row = new char[n];
            Arrays.fill(row, '.');
            row[queens[i]] = 'Q';
            board.add(new String(row));
        }
        return board;
    }


    public int[] dailyTemperatures(int[] temperatures) {
        int n = temperatures.length;
        int[] rs = new int[n];
        int maxValue = temperatures[n-1];
        int maxIdx = n-1;
        for (int i = n - 2; i >= 0; i--) {
            if (maxValue > temperatures[i]) {
                rs[i] = maxIdx;
            } else {
                rs[i] = 0;
                maxValue = temperatures[i];
                maxIdx = i;
            }
        }
        return rs;
    }


    /**
     * 打家劫舍3
     * @param root
     * @return
     */
//    public int rob(TreeNode root) {
//
//    }

    /**
     * 零钱兑换
     * @param coins
     * @param amount
     * @return
     */
    public int coinChange(int[] coins, int amount) {

        int[] rs = new int[amount+1];
        for (int i = 1; i <= amount; i++) {
            rs[i] = -1;
        }
        for (int i = 0; i < coins.length; i++) {
            for (int j = coins[i]; j <= amount; j++) {
                if (rs[j] == -1) {
                    rs[j] = rs[j - coins[i]] == -1 ? -1 : rs[j - coins[i]] + 1;
                } else {
                    if (rs[j-coins[i]] != -1) rs[j] = Math.min(rs[j], rs[j-coins[i]]+1);
                }
            }
        }
        return rs[amount];
    }

    /**
     * 戳气球
     * @param nums
     * @return
     */
// 1

    /**
     * 接雨水
     * @param height
     * @return
     */
    public int trap(int[] height) {
        int n = height.length;
        if (n < 3) return 0;
        int leftMax = height[0], rightMax = height[n-1];
        int i = 1, j = n-2, rs = 0;
        while (i <= j) {
            if (leftMax <= rightMax) {
                if (height[i] < leftMax) {
                    rs += leftMax - height[i];
                } else {
                    leftMax = height[i];
                }
                i++;
            } else {
                if (height[j] < rightMax) {
                    rs += rightMax - height[j];
                } else {
                    rightMax = height[j];
                }
                j--;
            }
        }
        return rs;
    }

    /**
     * 旋转有序数组
     * @param nums
     * @param target
     * @return
     */
    public int search(int[] nums, int target, int left, int right) {
        if (left >= right) return nums[left] == target ? left : -1;
        int mid = (left + right) / 2;
        if (nums[mid] > nums[left]) {
            int v = searchMid(nums, target, left, mid);
            if (v >= 0) return v;
            else return search(nums, target, mid+1, right);
        } else if (nums[mid] == nums[left]) {
            int v = nums[left] == target ? left : -1;
            if (v >= 0) return v;
            else return search(nums, target, mid + 1, right);
        } else {
            int v = searchMid(nums, target, mid+1, right);
            if (v >= 0) return v;
            else return search(nums, target, left, mid);
        }
    }

    public int searchMid(int[] nums, int target, int left, int right) {
        if (left >= right) return nums[left] == target ? left : -1;
        int mid = (left + right) / 2;
        if (nums[mid] > target) {
            return searchMid(nums, target, left, mid);
        } else if (nums[mid] < target) {
            return searchMid(nums, target, mid + 1, right);
        } else {
            return mid;
        }
    }

    /**
     * 下一个排列
     * @param nums
     */
    public void nextPermutation(int[] nums) {
        int n = nums.length;
        for (int i = n - 1; i > 0; i++) {
            if (nums[i] > nums[i - 1]) {
                swap(nums, i, i-1);
                return;
            }
        }
        int j = 0;
        while (j < n - j - 1) {
            swap(nums, j, n-j-1);
            j++;
        }
    }




    public void mergeSort(int[] arr, int left, int right, int[] tmp) {

        if (left >= right) return;
        int mid = (left + right) / 2;
        mergeSort(arr, left, mid, tmp);
        mergeSort(arr, mid+1, right, tmp);

        int i = left, j = mid+1;
        int k = left;
        while (i <= mid && j <= right) {
            if (arr[i] <= arr[j]) {
                tmp[k] = arr[i];
                i += 1;
            } else {
                j += 1;
                tmp[k] = arr[j];
            }
            k++;
        }
        while (i <= mid) {
            tmp[k] = arr[i];
            i+=1;
            k++;
        }
        while (j <= right) {
            tmp[k] = arr[j];
            j+=1;
            k++;
        }
        for (int m = left; m <= right; m++) {
            arr[m] = tmp[m];
        }
    }

    /**
     * 15. 三数之和
     * @param nums
     * @return
     */
    public List<List<Integer>> threeSum(int[] nums) {
        int n = nums.length;
        List<List<Integer>> rs = new ArrayList<>();
        if (n < 3) return rs;
        Arrays.sort(nums);
        for (int i = 0; i < n - 2; i++) {
            if (i == 0 || nums[i] != nums[i-1]) {
                Set<Integer> set = new HashSet<>();
                for (int k = i+1; k <= n-1; k++) {
                    if (set.contains(-nums[i] - nums[k])) {
                        List<Integer> r = new ArrayList<>();
                        r.add(nums[i]);
                        r.add(nums[k]);
                        r.add(-nums[i] - nums[k]);
                        rs.add(r);
                    } else {
                        set.add(nums[k]);
                    }
                }
            }
        }
        return rs;
    }

    /**
     * 11. 盛最多水的容器
     * @param height
     * @return
     */
    public int maxArea(int[] height) {
        int n = height.length;
        if (n < 2) return 0;
        int i = 0;
        int j = n-1;
        int rs = Math.min(height[i], height[j]) * (j-i);
        while (i < j - 2) {
            if (Math.min(height[i], height[j - 1]) < Math.min(height[j], height[i + 1])) {
                i++;
            } else {
                j--;
            }
            rs = Math.max(rs, Math.min(height[j], height[i]) * (j - i));
        }
        return rs;
    }

    /**
     * 回文子串
     * @param s
     * @return
     */
    public String longestPalindrome(String s) {
        int n = s.length();
        int i = 0;
        int j = 0;
        boolean[][] mat = new boolean[n][n];
        for (int p = 0; p < n; p++) {
            for (int q = 0; q < p; q++) {
                if (p - q == 1) {
                    mat[p][q] = s.charAt(p) == s.charAt(q);
                } else {
                    mat[p][q] = (s.charAt(p) == s.charAt(q)) && mat[p-1][q+1];
                }
                if (mat[p][q] && p - q > j - i) {
                    j = p;
                    i = q;
                }
            }
        }
        return s.substring(i, j+1);
    }

    /**
     * 堆排
     * @param arr
     */
    public void heapSort(int[] arr) {
        int n = arr.length;
        int lastNotLeaf = n / 2 - 1;
        for (int i = lastNotLeaf; i >= 0; i--) {
            adjustMaxHeap(arr, i, n-1);
        }
        for (int i = 1; i < n; i++) {
            swap(arr, 0, n-i);
            adjustMaxHeap(arr, 0, n-i-1);
        }
    }


    public void adjustMaxHeap(int[] arr, int left, int right) {
        int tmp = arr[left];
        int k = left;
        for (int i=left*2+1; i <= right; i=i*2+1) {
            if (i+1<=right && arr[i+1] > arr[i]) i++;
            if (arr[i] > tmp) {
                arr[k] = arr[i];
                k = i;
            } else {
                break;
            }
        }
        arr[k] = tmp;
    }


    public void quickSort(int[] arr, boolean recursive) {
        if (recursive) {
            recursiveQuickSort(arr, 0, arr.length - 1);
        } else {
            unRecursiveQuickSort(arr, 0, arr.length - 1);
        }
    }


    public void recursiveQuickSort(int[] arr, int begin, int end) {
        if (begin >= end) return;
        int i = begin;
        int j = end;
        int base = arr[i];
        while (i < j) {
            while (i < j && arr[j] >= base) j--;
            if (i < j) {
                arr[i] = arr[j];
                i++;
            }
            while (i < j && arr[i] <= base) i++;
            if (i < j) {
                arr[j] = arr[i];
                j--;
            }
        }
        arr[i] = base;
        recursiveQuickSort(arr, begin, i-1);
        recursiveQuickSort(arr, i+1, end);
    }


    public void unRecursiveQuickSort(int[] arr, int begin, int end) {
        Stack<String> stack = new Stack<>();
        stack.push(begin + "," + end);
        while (!stack.isEmpty()) {
            String[] idxs = stack.pop().split(",");
            int b1 = Integer.parseInt(idxs[0]);
            int e1 = Integer.parseInt(idxs[1]);
            int i = b1;
            int j = e1;
            int base = arr[i];
            while (i < j) {
                while (i < j && arr[j] >= base) j--;
                if (i < j) {
                    arr[i] = arr[j];
                    i++;
                }
                while (i < j && arr[i] <= base) i++;
                if (i < j) {
                    arr[j] = arr[i];
                    j--;
                }
            }
            arr[i] = base;
            if (b1 < i-1) stack.push(b1 + "," + (i-1));
            if (i+1 < end) stack.push((i+1) + "," + end);
        }
    }



    public void swap(int[] arr, int i, int j) {
        int v = arr[i];
        arr[i] = arr[j];
        arr[j] = v;
    }
}


class Main {


    public static void main(String[] args) {
        int[] arr = new int[]{3,2,1,4,5,6};
        ListSort ls = new ListSort();
        ls.heapSort(arr);

        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]+",");
        }
        System.out.println("=====");

        System.out.println(ls.longestPalindrome("cbbd"));
        System.out.println(ls.maxArea(new int[]{1,8,6,2,5,4,8,3,7}));
        System.out.println(ls.threeSum(new int[]{-1,0,1,2,-1,-4}));

        System.out.println(ls.search(new int[]{1}, 0, 0, 0));
        System.out.println(ls.trap(new int[]{0,1,0,2,1,0,1,3,2,1,2,1}));

        System.out.println(ls.coinChange(new int[]{1,2,5},11));
        System.out.println(Arrays.toString(ls.dailyTemperatures(new int[]{73,74,75,71,69,72,76,73})));

        System.out.println(ls.firstMissingPositive(new int[]{3,4,-1,1}));
        System.out.println(ls.getPermutation(4, 9));
        System.out.println(ls.minWindow("ADOBECODEBANC", "ABC"));
    }
}

