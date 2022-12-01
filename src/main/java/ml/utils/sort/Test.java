package ml.utils.sort;

import javafx.util.Pair;

import java.util.*;

/**
 * author: renjian
 * Datetime: 2021/1/4 下午11:14
 * email: renjian04@meituan.com
 * Description:
 */
class TreeNode {
      int val;
      TreeNode left;
      TreeNode right;
      TreeNode(int x) { val = x; }
 }

public class Test {

    /**
     * 前序遍历   迭代式
     * @param root
     * @return
     */
    public List<Integer> preorderTraversal(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        Stack<TreeNode> stack = new Stack<>();
        if (root != null) stack.add(root);
        while (! stack.isEmpty()) {
            TreeNode tmp = stack.pop();
            rs.add(tmp.val);

            if (tmp.right != null) stack.add(tmp.right);
            if (tmp.left != null) stack.add(tmp.left);
        }
        return rs;

    }

    /**
     * 中序遍历
     * @param root
     * @return
     */
    public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        if (root == null) return rs;
        Stack<TreeNode> stack = new Stack<>();
        while (!stack.isEmpty() || root != null) {
            while (root != null) {
                stack.add(root);
                root = root.left;
            }
            root = stack.pop();
            rs.add(root.val);
            root = root.right;
        }
        return rs;
    }

    /**
     * 后序遍历
     * @param root
     * @return
     */
    public List<Integer> postorderTraversal(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        if (root == null) return rs;
        Stack<TreeNode> stack = new Stack<>();
        TreeNode prev = null;
        while (!stack.isEmpty() || root != null) {
            while (root != null) {
                stack.add(root);
                root = root.left;
            }
            root = stack.peek();
            if (root.right == null || root.right == prev) {
                prev = root;
                root = null;
                rs.add(stack.pop().val);
            } else {
                root = root.right;
            }
        }
        return rs;
    }


    public static int trap1(int[] height) {
        Stack<Integer> stack = new Stack<>();
        int n = height.length;
        int leftM = 0, rightM = 0, rs=0;
        int left = 1, right=n-1;
        for (int i=1; i<n-1; i++) {
//            leftM = Math.max(leftM, height[i-1]);
//            left[i] = leftM;
            System.out.println(height[right+1]);
            if (height[left-1] < height[right+1]) {
                leftM = Math.max(leftM, height[left-1]);
                if (leftM > height[left]) rs += leftM - height[left];
                left++;
            } else {
                rightM = Math.max(rightM, height[right+1]);
                if (rightM > height[right]) rs += rightM - height[right];
                right--;
            }

//            if (height[left-1] <height[right+1]) {
//
//                int m = height[left-1];
//            } else {
//
//            }
//            rightM = Math.max(rightM, height[n-i]);
//            right[n-1-i] = rightM;


        }
//        int rs = 0;
//        for (int i=1; i<n; i++) {
//            leftM = Math.max(leftM, height[i-1]);
//            int diff = Math.min(leftM, right[i]);
//            if (diff > height[i]) {
//                rs += (diff - height[i]);
////                System.out.println(i + ", " + left[i] + ", " + right[i] + ", "+height[i]);
//            }
//
//        }
        return rs;
    }

    public static int trap(int[] height) {
        int n = height.length;
//        int[] left = new int[n];
        int[] right = new int[n];
        int leftM = 0;
        int rightM = 0;
        for (int i=1; i<n; i++) {
//            leftM = Math.max(leftM, height[i-1]);
//            left[i] = leftM;
            rightM = Math.max(rightM, height[n-i]);
            right[n-1-i] = rightM;

        }
        int rs = 0;
        for (int i=1; i<n; i++) {
            leftM = Math.max(leftM, height[i-1]);
            int diff = Math.min(leftM, right[i]);
            if (diff > height[i]) {
                rs += (diff - height[i]);
//                System.out.println(i + ", " + left[i] + ", " + right[i] + ", "+height[i]);
            }

        }
        return rs;
    }

    public static List<List<Integer>> levelOrder(TreeNode root) {
        List<List<Integer>> rs = new ArrayList<List<Integer>>();
        if (root == null) return rs;
        // List<TreeNode> tmp = new ArrayList<TreeNode>();
        // tmp.add(root);
        // while (tmp.size() > 0) {
        //     ArrayList<Integer> tmp1 = new ArrayList<Integer>();
        //     ArrayList<TreeNode> tmp2 = new ArrayList<TreeNode>();
        //     for (TreeNode i: tmp) {
        //         tmp1.add(i.val);
        //         if (i.left != null) tmp2.add(i.left);
        //         if (i.right != null) tmp2.add(i.right);
        //         // tmp.remove(i);
        //     }
        //     rs.add(tmp1);
        //     tmp = tmp2;
        // }
        Queue<TreeNode> stack = new LinkedList<>();

        stack.offer(root);
        int c = 1;
        int idx = 0;
        int c1 = 0;
        List<Integer> tmp = new ArrayList<>();
        while (!stack.isEmpty() || !tmp.isEmpty()) {
            if (idx < c) {
                TreeNode n = stack.poll();
                System.out.println(n.val);
                tmp.add(n.val);
                if (n.left != null) {
                    stack.offer(n.left);
                    c1 += 1;
                }
                if (n.right != null) {
                    stack.offer(n.right);
                    c1 += 1;
                }
                idx += 1;
            } else {
                rs.add(tmp);
                System.out.println(Arrays.toString(tmp.toArray()));
                tmp = new ArrayList<>();
                c = c1;
                System.out.println(c);
                idx = 0;
                c1 = 0;
            }

        }
        return rs;
    }

    public static int coinChange(int[] coins, int amount) {
        int n = coins.length;
        int[] dp = new int[amount+1];
        dp[0] = 0;

        for (int i=1; i<=amount; i++) {
            int minV = Integer.MAX_VALUE;
            boolean flag = false;
            for (int j=0; j<n; j++) {
                if (i-coins[j] >=0 && dp[i-coins[j]] >= 0) {
                    minV = Math.min(minV, dp[i-coins[j]]);
                    flag = true;
                }
            }
            dp[i] = flag ? minV+1 : -1;
            System.out.println("i: " + i + ", v: " + dp[i]);
        }
        return dp[amount];
    }

    public static int uniquePaths(int m, int n) {

        int[][] paths = new int[m][n];

        Queue<Integer> queue = new LinkedList<Integer>();
        queue.offer(m*n-1);
        paths[m-1][n-1] = 1;
        while (queue.size() > 0) {
            int v = queue.poll();
            int r = v / n;
            int c = v % n;
            if (r>0 && paths[r-1][c] == 0) {
                paths[r-1][c] = c<n-1 ? paths[r][c]+paths[r-1][c+1] : paths[r][c];
                System.out.println("r: " + (r-1) + " c: " + c + " v: " + paths[r-1][c]);
                if (r-1 == 0 && c==5) {
                    System.out.println(paths[r][c] + " , " + paths[r-1][c+1]);
                }
                queue.offer((r-1)*n+c);
            }
            if (c>0 && paths[r][c-1] == 0) {
//                System.out.println("r: " + r + ", c: "+c);
                paths[r][c-1] = r<m-1 ? paths[r][c]+paths[r+1][c-1] : paths[r][c];
                System.out.println("r: " + r + " c: " + (c-1) + " v: " + paths[r][c-1]);
                if (r == 0 && c-1==5) {
                    System.out.println(paths[r][c] + " , " + paths[r+1][c]);
                }
                queue.offer(r*n+c-1);
            }
        }
        return paths[0][0];
    }

    public static int pow(int x, int n) {
        if (n == 0) return 1;
        if (n == 1) return x;

        int p = n / 2;
        int q = n % 2;

        int v = pow(x, p);
        return v * v * pow(x, q);
    }

    public static  int maxProfit(int[] prices) {
        if (prices.length <=1) return 0;
        int minPrice = Integer.MAX_VALUE, maxDiff = 0;
        Queue<Integer> queue = new LinkedList<Integer>();

        for (int i=1; i<prices.length; i++) {
            minPrice = Math.min(minPrice, prices[i-1]);
            maxDiff = Math.max(maxDiff, prices[i] - minPrice);
            System.out.println(maxDiff);
        }
        // if (maxDiff < 0) maxDiff = 0;
        return maxDiff;
//        System.out.println();

    }

    public static void main(String[] args) {
//        char[][] board = new char[4][5];
//        board[0] = new char[]{'1','1','1','1','0'};
//        board[1] = new char[]{'1','1','0','1','0'};
//        board[2] = new char[]{'1','1','0','0','0'};
//        board[3] = new char[]{'0','0','0','0','0'};

//        char[][] board = new char[3][3];
//        board[0] = new char[]{'1','1','1'};
//        board[1] = new char[]{'0','1','0'};
//        board[2] = new char[]{'1','1','1'};
//
//        int rs = numIslands(board);
//        System.out.println(rs);
//        int[] prices = new int[]{7,1,5,3,6,4};
//        maxProfit(prices);
//        uniquePaths(3, 7);
//        coinChange(new int[]{1,2,5}, 11);
//        TreeNode root = new TreeNode(3);
//        TreeNode a1 = new TreeNode(15);
//        TreeNode a2 = new TreeNode(7);
//        TreeNode b1 = new TreeNode(9);
//        TreeNode b2 = new TreeNode(20);
//        b2.left = a1;
//        b2.right = a2;
//        root.left = b1;
//        root.right = b2;
//        levelOrder(root);
//        int r = trap(new int[]{0,1,0,2,1,0,1,3,2,1,2,1});
//        System.out.println(r);
//        Arrays.sort();

//        for (int i=0; i<10; i++) {
//            System.out.println(i);
//            i++;
//        }
        System.out.println(pow(2, 30));


    }



    public static int numIslands(char[][] grid) {
        int rs = 0;
        int m = grid.length, n = grid[0].length;

        for (int i=0; i<m; i++) {
            for (int j=0; j<n; j++) {
                if (grid[i][j] == '1') {
                    rs += 1;
                    System.out.println("i:" + i + ", j: "+ j);
                    Queue<Pair<Integer, Integer>> queue = new LinkedList<Pair<Integer, Integer>>();
                    queue.offer(new Pair<>(i, j));
                    while (queue.size() > 0) {
                        Pair<Integer, Integer> p = queue.poll();
                        int l = p.getKey();
                        int k = p.getValue();
                        grid[l][k] = '0';
                        System.out.println("l:" + l + ", k: "+ k);
                        if (l<m-1 && grid[l+1][k] == '1') queue.offer(new Pair<>(l+1, k));
                        if (l>0 && grid[l-1][k] == '1') queue.offer(new Pair<>(l-1, k));
                        if (k<n-1 && grid[l][k+1] == '1') queue.offer(new Pair<>(l, k+1));
                        if (k>0 && grid[l][k-1] == '1') queue.offer(new Pair<>(l, k-1));
                    }
                }

//                for (int p=0; p<4; p++) {
//                    for (int q=0; q<5; q++) {
//                        System.out.print(grid[p][q]);
//                    }
//                    System.out.println("");
//                }
            }
        }
        return rs;
    }


    public static boolean backStack(char[][] board, int row, int col, int state) {
        if (row == 9) return true;
        if (board[row][col] != '.') {
            boolean f;
            state += 1;
            if (col < 8) {
                f = backStack(board, row, col+1, state);
            } else {
                f = backStack(board, row+1, 0, state);
            }
            return f;
        } else {
            int c = 0;
            for (int m=1; m<=9; m++) {
                boolean flag = true;
                char k = String.valueOf(m).charAt(0);
                for (int i=0; i<9; i++) {
                    if (board[row][i] == k ||
                            board[i][col] == k ||
                            board[i/3+row/3][i%3+col/3] == k) {
                        flag = false;
                        break;
                    } else {
                        continue;
                    }
                }
                if (flag) {
                    board[row][col] = k;
                    if (row==0 && col==8) {
//                        System.out.println(row+", "+col+", "+k + ", "+flag);
                        for (int i=0; i<9; i++) {
                            StringBuffer sb = new StringBuffer("");
                            for (int j=0; j<9; j++) {
                                sb.append(board[i][j]).append(",");

                            }
                            System.out.println(sb.toString());
                        }
                    }
                    boolean f;
                    state += 1;
                    if (col < 8) {
                        f = backStack(board, row, col+1, state);
                    } else {
                        f = backStack(board, row+1, 0, state);
                    }
                    if (!f) {
                        board[row][col] = '.';
                        c+=1;
                    }
                } else {
                    c += 1;
//                    System.out.println(row+", "+col+", "+k + ", "+flag);
                }
                if (state >= 81) break;
            }
            if (c == 9) {
                return false;
            } else {
                return true;
            }
        }
    }
}
