package ml.utils.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * author: renjian
 * Datetime: 2021/4/28 上午10:06
 * email: renjian04@meituan.com
 * Description:
 */
public class Merge {


    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int m = nums1.length, n = nums2.length;

        if ((m+n) % 2 == 0) {
            int idx = (m+n) / 2;
            return (getMinK(nums1, nums2, idx) + getMinK(nums1, nums2, idx+1)) / 2.0;
        } else {
            int idx = (m+n) / 2 + 1;
            return getMinK(nums1, nums2, idx);
        }
    }

    public static int getMinK(int[] nums1, int[] nums2, int k) {
        // [1,2,3]
        // [2,3,4,5]  k=4
        int m = nums1.length, n = nums2.length;
        int begin1 = 0, begin2 = 0;
        while (k > 1) {
            if (begin1 < m && begin2 < n) {
                int pos = k / 2 - 1;
                int pos1 = Math.min(begin1 + pos, m-1);
                int pos2 = Math.min(begin2 + pos, n-1);
                if (nums1[pos1] <= nums2[pos2]) {
                    k -= (pos1 - begin1 + 1);
                    begin1 = pos1 + 1;
                } else {
                    k -= (pos2 - begin2 + 1);
                    begin2 = pos2 + 1;
                }
            } else if (begin1 < m) {
                return nums1[begin1+k-1];
            } else {
                return nums2[begin2+k-1];
            }
        }
        int rs;
        if (begin1 < m && begin2 < n) {
            rs = nums1[begin1] < nums2[begin2] ? nums1[begin1] : nums2[begin2];
        } else if (begin1 < m) {
            rs = nums1[begin1];
        } else {
            rs = nums2[begin2];
        }
        return rs;
    }


    public static void main(String[] args) {
        int[] nums1 = new int[]{1,2};
        int[] nums2 = new int[]{3,4};

//        System.out.println(getMinK(nums1, nums2, 3));
        System.out.println(pow1(3, 5));
    }


    public static double pow1(double x, int k) {
        if (k < 0) return 1 / pow1(x, -k);
        if (k == 0) return 1d;
        if (k == 1) return x;
        if (k > 1) {
            double v = pow1(x, k/2);
            if (k % 2 == 0) {

                return v*v;
            } else {
                return v*v*x;
            }
        }
        return 0;
    }


    public List<List<Integer>> combinationSum(int[] candidates, int target) {
        int n = candidates.length;
        List<List<Integer>> rs = new ArrayList<>();
        List<Integer> tmp = new ArrayList<>();
        dfs(rs, tmp, candidates, target);

        return rs;
    }

    public void dfs(List<List<Integer>> rs, List<Integer> tmp, int[] candidates, int target) {
        if (target <= 0) {
            if (target == 0) rs.add(new ArrayList<>(tmp));
            return;
        }
        int n = candidates.length;
        for (int i=0; i<n; i++) {
            int v = tmp.size();
            if (v > 0 && tmp.get(v-1) > candidates[i]) continue;
            tmp.add(candidates[i]);
            dfs(rs, tmp, candidates, target-candidates[i]);
            tmp.remove(tmp.size()-1);
        }
    }



    public void solveSudoku(char[][] board) {
        dfs(board, 0,0);
    }

    public int dfs(char[][] board, int i, int j) {
        if (i == 9) {
//            System.out.println("i:"+i+" j:"+j);
            return 1;
        }
        if (board[i][j] != '.') {
//            System.out.println("i:"+i+" j:"+j);
            return dfs(board, getNextR(i,j), getNextC(i,j));
        }
        for (int k=1; k<=9; k++) {
            if (valid(board, i, j, Character.forDigit(k,10))) {
                int v = dfs(board, getNextR(i,j), getNextC(i,j));
//                System.out.println("i:"+i+" j:"+j + " v:"+v);
                if (v == 1) {
                    return 1;
                } else {
                    board[i][j] = '.';
                }
            }
        }
        return 0;
    }

    public boolean valid(char[][] board, int i, int j, char v) {
        for (int m=0; m<9; m++) {
            if (m!=j && board[i][m] == v) return false;
        }
        for (int m=0; m<9; m++) {
            if (m != i && board[m][j] == v) return false;
        }
        int m = i / 3, n = j / 3;
        for (int p=m*3; p<(m+1)*3; p++) {
            for (int q=n*3; q<(n+1)*3; q++) {
                if ((p!=i || q!=j) && board[p][q]==v) return false;
            }
        }
        board[i][j] = v;
        return true;
    }

    public int getNextR(int i, int j) {
        return j==8 ? i+1 : i;
    }
    public int getNextC(int i, int j) {
        return j==8 ? 0 : j+1;
    }


    public int minDistance(String word1, String word2) {
        return 0;
    }


    public String convert(String s, int numRows) {
        char[] arr = s.toCharArray();
        int n = arr.length, l = numRows*2-2;
        int c = n / l;
        if (n % l > 0) {
            for (int i=0; i<l-n%l; i++) {
                s = s + "0";
            }
            c += 1;
        }
        StringBuffer sb = new StringBuffer();

        for (int i=0; i<c; i++) {
            char v = s.charAt(i*l);
            if (v != '0') sb.append(v);
        }
        for (int j=1; j<numRows-1; j++) {
            for (int i=0; i<c; i++) {
                char v1 = s.charAt(i*l+j), v2 = s.charAt((i+1)*l-j);
                if (v1 != '0') sb.append(v1);
                if (v2 != '0') sb.append(v2);
            }
        }
        for (int i=0; i<c; i++) {
            char v = s.charAt(i*l+numRows-1);
            if (v != '0') sb.append(v);
        }

        return sb.toString();

    }



    public int[][] reconstructQueue(int[][] people) {
        int n = people.length;
        Arrays.sort(people, new Comparator<int[]>() {
            @Override
            public int compare(int[] o1, int[] o2) {
                return o1[0]==o2[0] ? o2[0]-o1[0] : o1[1]-o2[1];
            }
        });
        List<int[]> list = new ArrayList<>();
        for (int[] mid: people) {
            list.add(mid[1], mid);
        }
        return list.toArray(new int[n][2]);
    }


    public int maxSquare(int[] heights) {
        int n = heights.length;
        int[] tmp = new int[n-1];
        int i=0, j=n-1, idx=0;
        while (i < j) {
            if (heights[i]<heights[j]) {
                i += 1;
                tmp[idx] = heights[i];
            } else {
                j -= 1;
                tmp[idx] = heights[j];
            }
            idx += 1;
        }
        int rs = heights[i], mi = heights[i], c=2;
        for (int k=n-2; k>=0; k--) {
            mi = Math.min(mi, tmp[k]);
            rs = Math.max(rs, mi*c);
            c++;
        }
        return rs;
    }



}



//class Solution {
//    public ListNode reverseKGroup(ListNode head, int k) {
//
//    }
//}



