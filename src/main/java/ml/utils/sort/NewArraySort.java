package ml.utils.sort;

import javafx.util.Pair;

import java.util.*;

/**
 * author: renjian
 * Datetime: 2021/4/28 下午4:57
 * email: renjian04@meituan.com
 * Description:
 */
public class NewArraySort {
    Map<Integer, Integer> idxMap = new HashMap<>();





    public double findTopKSordedArray(int[] nums1, int[] nums2, int l1, int l2, int k) {

        int m = nums1.length, n = nums2.length;
        while (true) {
            int half = k / 2;
            if (l1 >= m) return nums2[l2 + k -1];
            if (l2 >= n) return nums1[l1 + k - 1];
            if (half == 1) return nums1[l1] < nums2[l2] ? nums1[l1] : nums2[l2];
            int idx1 = l1 + half - 1, idx2 = l2 + half - 1;
            if (idx1 >= m) idx1 = m-1;
            if (idx2 >= n) idx2 = n-1;

            if (nums1[idx1] < nums2[idx2]) {
                k -= (idx1 - l1 + 1);
                l1 = idx1 + 1;
            } else {
                k -= (idx2 - l2 + 1);
                l2 = idx2 + 1;
            }

        }
    }




    public void preOrder(TreeNode node) {
        Deque<TreeNode> deq = new LinkedList<>();

        while (!deq.isEmpty() || null != node) {
            while (null != node) {
                System.out.println(node.val);
                deq.push(node);
                node = node.left;
            }
            node = deq.pop();
            node = node.right;
        }

    }

    public void midOrder(TreeNode node) {
        Deque<TreeNode> deq = new LinkedList<>();

        while (!deq.isEmpty() || null != node) {
            while (null != node) {
                deq.push(node);
                node = node.left;
            }
            node = deq.pop();
            System.out.println(node.val);
            node = node.right;
        }
    }


    public void afterOrder(TreeNode node) {
        Deque<TreeNode> deq = new LinkedList<>();
        TreeNode back = node;
        while (!deq.isEmpty() || null != node) {
            while (null != node) {
                deq.push(node);
                node = node.left;
            }
            node = deq.pop();
            if (node.right == null || node.right == back) {
                System.out.println(node.val);
                back = node;
                node = null;
            } else {
                deq.push(node);
                node = node.right;
            }
        }
    }


    public int maxLength(int[] arr) {

        int n = arr.length, ans = 0;
        if (n == 0) return ans;
        Set<Integer> eles = new HashSet<>();
        for (int i = 0; i < n; i++) {
            eles.add(arr[i]);
        }
        for (int i = 0; i < n; i++) {
            if (eles.contains(arr[i]-1)) continue;
            int c = 1, v = arr[i];
            while (eles.contains(v+1)) {
                c++;
                v++;
            }
            ans = Math.max(ans, c);
        }
        return ans;
    }

    public TreeNode deleteNode(TreeNode root, int key) {
        if (null == root) return root;
        Deque<TreeNode> stack = new LinkedList<>();
        stack.push(root);
        TreeNode pre = null;
        while (!stack.isEmpty()) {
            TreeNode node = stack.pop();
            if (node.val == key) {
                if (null == deleteNodeRec(node)) {
                    if (null == pre) return pre;
                    else {
                        if (pre.left == node) pre.left = null;
                        else pre.right = null;
                    }
                }
            } else if (node.val < key) {
                if (node.right != null) stack.push(node.right);
            } else {
                if (node.left != null) stack.push(node.left);
            }
            pre = node;
        }
        return root;
    }

    public TreeNode deleteNodeRec(TreeNode root) {
        if (null != root.right) {
            TreeNode n = deleteNodeRec(root.right);
            root.val = root.right.val;
            if (null == n) root.right = null;
            return root;
        }
        if (null != root.left) {
            TreeNode n = deleteNodeRec(root.left);
            root.val = root.left.val;
            if (null == n) root.left = null;
            return root;
        }
        return null;
    }


    /**
     * 490  499  505
     */

    /**
     * 695 岛屿的最大面积
     * @param grid
     * @return
     */
    public int maxAreaOfIsland(int[][] grid) {
        int m = grid.length, n = grid[0].length, ans = 0;
        Queue<Integer> que = new LinkedList<>();
        for (int i=0; i<m; i++) {
            for (int j = 0; j < n; j++) {
                int c = grid[i][j], tmp = 0;
                if (c != 0) {
                    que.add(i * n + j);
                    grid[i][j] = 0;
                    while (!que.isEmpty()) {
                        int v = que.poll();
                        int x = v / n, y = v % n;
                        tmp++;
                        if (x-1 >= 0 && grid[x-1][y] == 1) {
                            que.add((x-1)*n+y);
                            grid[x-1][y] = 0;
                        }
                        if (x+1<m && grid[x+1][y] == 1) {
                            que.add((x+1)*n+y);
                            grid[x+1][y] = 0;
                        }
                        if (y-1>=0 && grid[x][y-1] == 1) {
                            que.add(x*n+y-1);
                            grid[x][y-1] = 0;
                        }
                        if (y+1<n && grid[x][y+1] == 1) {
                            que.add(x*n+y+1);
                            grid[x][y+1] = 0;
                        }

                    }
                }
                ans = Math.max(ans, tmp);
            }
        }
        return ans;

    }

    /**
     * 463 岛屿的周长
     * @param grid
     * @return
     */
    public int islandPerimeter(int[][] grid) {
        int m = grid.length, n = grid[0].length, ans = 0;
        for (int i=0; i<m; i++) {
            for (int j = 0; j < n; j++) {
                int c = grid[i][j];
                if (c != 0) {
                    if (i-1 < 0 || (i-1>=0 && grid[i-1][j] == 0)) ans++;
                    if (i+1 >= m || (i+1<m && grid[i+1][j] == 0)) ans++;
                    if (j-1 < 0 || (j-1>=0 && grid[i][j-1] == 0)) ans++;
                    if (j+1 >= n || (j+1<n && grid[i][j+1] == 0)) ans++;
                }
            }
        }
        return ans;
    }

    /**
     * 200 岛屿数量
     * @param grid
     * @return
     */
    public int numIslands(char[][] grid) {
        int m = grid.length, n = grid[0].length, ans = 0;
        for (int i=0; i<m; i++) {
            for (int j = 0; j < n; j++) {
                char c = grid[i][j];
                if (c != '0') {
                    ans++;
                    numIslandsDfs(grid, i, j);
                }
            }
        }
        return ans;
    }

    public void numIslandsDfs(char[][] grid, int x, int y) {
        int m = grid.length, n = grid[0].length;
        int[][] dirs = {{0, -1}, {0, 1}, {-1, 0}, {1, 0}};
        grid[x][y] = '0';
        for (int[] dir: dirs) {
            int nx = x + dir[0], ny = y + dir[1];
            if (nx >=0 && nx < m && ny >=0 && ny < n && grid[nx][ny] != '0') {
                numIslandsDfs(grid, nx, ny);
            }
        }
    }


    public int sumRootToLeaf(TreeNode root) {
        if (root == null) return 0;
        return sumRootToLeafRec(root, 0);
    }

    public int sumRootToLeafRec(TreeNode root, int par) {
        int ans = (par << 1) + root.val;
        if (root.left == null && root.right == null) return ans;
        int rs = 0;
        if (root.left != null) {
            rs += sumRootToLeafRec(root.left, ans);
        }
        if (root.right != null) {
            rs += sumRootToLeafRec(root.right, ans);
        }
        return rs;
    }



    /** 699 掉落的方块
     * 在二维平面上的 x 轴上，放置着一些方块。
     * 给你一个二维整数数组 positions ，其中 positions[i] = [lefti, sideLengthi] 表示：第 i 个方块边长为 sideLengthi ，其左侧边与 x 轴上坐标点 lefti 对齐。
     * 每个方块都从一个比目前所有的落地方块更高的高度掉落而下。方块沿 y 轴负方向下落，直到着陆到 另一个正方形的顶边 或者是 x 轴上 。一个方块仅仅是擦过另一个方块的左侧边或右侧边不算着陆。一旦着陆，它就会固定在原地，无法移动。
     * 在每个方块掉落后，你必须记录目前所有已经落稳的 方块堆叠的最高高度 。
     * 返回一个整数数组 ans ，其中 ans[i] 表示在第 i 块方块掉落后堆叠的最高高度。
     * @param
     * @return
     */
//    public List<Integer> fallingSquares(int[][] positions) {
//
//
//    }


    public List<Integer> preOrderTree(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        if (root == null) return rs;

        Deque<TreeNode> stack = new LinkedList<>();
        TreeNode node = root;
        while (!stack.isEmpty() || node != null) {
            while (node != null) {
                rs.add(node.val);
                stack.push(node);
                node = node.left;
            }
            node = stack.pop();
            node = node.right;
        }
        return rs;
    }


    public List<Integer> inOrderTree(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        if (root == null) return rs;

        Deque<TreeNode> stack = new LinkedList<>();
        TreeNode node = root;
        while (!stack.isEmpty() || node != null) {
            while (node != null) {
                stack.push(node);
                node = node.left;
            }
            node = stack.pop();
            rs.add(node.val);
            node = node.right;
        }
        return rs;
    }



    public List<Integer> afterOrderTree(TreeNode root) {
        List<Integer> rs = new ArrayList<>();
        if (root == null) return rs;

        Deque<TreeNode> stack = new LinkedList<>();
        TreeNode node = root, pre = root;
        while (!stack.isEmpty() || node != null) {
            while (node != null) {
                stack.push(node);
                node = node.left;
            }
            node = stack.pop();
            if (node.right != null && pre != node.right) {
                stack.push(node);
                node = node.right;
            } else {
                rs.add(node.val);
                pre = node;
                node = null;
            }
        }
        return rs;
    }





    public void solveSudoku(char[][] board) {
        solveSudoku(board, 0, 0);
    }



    public boolean solveSudoku(char[][] board, int x, int y) {
        int m = board.length;
        if (x == m) return true;
        int nx = x, ny = y;
        if (y == m-1) {
            nx++;
            ny = 0;
        } else {
            ny++;
        }
        if (board[x][y] != '.') return solveSudoku(board, nx, ny);
        for (int i=1; i<9; i++) {
            char c = String.valueOf(i).charAt(0);
            boolean f = true;
            int d1 = x / 3, d2 = y / 3;
            for (int j=0; j<m; j++) {
                if (board[x][j] == c || board[j][y] == c || board[d1*3+j/3][d2*3+j%3] == c) {
                    f = false;
                    break;
                }
            }
            if (f) {
                board[x][y] = c;
                boolean v = solveSudoku(board, nx, ny);
                if (v) {
                    return v;
                } else {
                    board[x][y] = '.';
                }
            }
        }
        return false;
    }

    public boolean canIWin(int max, int total) {
        boolean[] visited = new boolean[max];
        return canIWinDp(visited, 0, max, total);
    }


    public boolean canIWinDp(boolean[] visited, int idx, int max, int total) {
        if (total <= 0) {
            return idx % 2 == 0 ? true : false;
        }
        for (int i=0; i<max; i++) {
            if (! visited[i]) {
                int v = i+1;
                visited[i] = true;
                boolean r =  canIWinDp(visited, idx+1, max, total-v);
                visited[i] = false;
                if (r) return r;
            }
        }
        return false;
    }

    public double myPow(double x, int n) {

        double rs = 1, std = x;
        while (n >= 1) {
            if (n % 2 == 1) {
                rs *= std;
            }
            std *= std;
            n /= 2;
        }
        return rs;
    }

//    5  2 1    7 3 1
//
//    2 2 1   n3 * n3 * 1

    public int ladderLength(String beginWord, String endWord, List<String> wordList) {

        return 0;
    }

    public int minimumBuckets(String street) {
        // ".H.H.H."
        return 0;
    }

    public int trapRainWater(int[][] heightMap) {

        int m = heightMap.length, n = heightMap[0].length;
        PriorityQueue<int[]> pq = new PriorityQueue<>((o1, o2) -> o1[1] - o2[1]);
        boolean[][] visited = new boolean[m][n];
        for (int i=0; i<m; i++) {
            pq.add(new int[]{i*n, heightMap[i][0]});
            pq.add(new int[]{i*n+n-1, heightMap[i][n-1]});
            visited[i][0] = true;
            visited[i][n-1] = true;
        }
        for (int i=1; i<n-1; i++) {
            pq.add(new int[]{i, heightMap[0][i]});
            pq.add(new int[]{(m-1)*n+i, heightMap[m-1][i]});
            visited[0][i] = true;
            visited[m-1][i] = true;
        }

        int rs = 0;
        int[] dir1 = {0, 0, -1, 1};
        int[] dir2 = {-1, 1, 0, 0};
        while (! pq.isEmpty()) {
            int[] v = pq.poll();
            for (int i=0; i<4; i++) {
                int r = v[0] / n + dir1[i], c = v[0] % n + dir2[i];
                if (r >=0 && r < m && c >=0 && c < n && !visited[r][c]) {
                    int s = Math.min(heightMap[r][c], v[1]);
                    pq.add(new int[]{r*n+c, s});
                    rs += (s - heightMap[r][c]);
                    visited[r][c] = true;
                }
            }
        }
        return rs;
    }


    public double findMedianSordedArrays(int[] nums1, int[] nums2) {
        int m = nums1.length, n = nums2.length;
        if ((m+n) % 2 == 1) {
            return findTopKSordedArrays(nums1, 0, nums2, 0, (m+n)/2+1);
        } else {
            double v1 = findTopKSordedArrays(nums1, 0, nums2, 0, (m+n)/2);
            double v2 = findTopKSordedArrays(nums1, 0, nums2, 0, (m+n)/2+1);
            return (v1 + v2) / 2;
        }
    }

    public double findTopKSordedArrays(int[] nums1, int l1, int[] nums2, int l2, int k) {

        int m = nums1.length, n = nums2.length;
        if (k == 1) return Math.min(nums1[l1], nums2[l2]);
        if (l1 >= m) return nums2[l2+k-1];
        if (l2 >= n) return nums1[l1+k-1];

        int hl = k / 2;
        int idx1 = Math.min(m-1, l1+hl-1), idx2 = Math.min(n-1, l2+hl-1);
        if (nums1[idx1] < nums2[idx2]) {
            l1 += hl;
        } else {
            l2 += hl;
        }
        k -= hl;
        return findTopKSordedArrays(nums1, l1, nums2, l2, k);

    }


    public int lengthOfLongestSubString(String s) {
        int n = s.length(), i = 0, j = 0;
        if (n == 0) return 0;
        Set<Character> set = new HashSet<>();
        int rs = 1;
        while (i < n) {
            while (j < n && !set.contains(s.charAt(j))) {
                set.add(s.charAt(j));
                j++;
            }
            rs = Math.max(rs, j-i);
            set.remove(s.charAt(i));
            i++;
        }
        return rs;
    }

    public int maxValue(int[][] grid) {
        int m = grid.length, n = grid[0].length;

        int[][] vs = new int[m][n];
        return maxValueDp(grid, vs, 0, 0);
    }

    public int maxValueDp(int[][] grid, int[][] vs, int i, int j) {
        if (vs[i][j] == 0) {
            int m = grid.length, n = grid[0].length, tmp = 0;
            if (i+1<m) {
                tmp = Math.max(maxValueDp(grid, vs, i+1, j), tmp);
            }
            if (j+1 < n) {
                tmp = Math.max(maxValueDp(grid, vs, i, j+1), tmp);
            }
            vs[i][j] = tmp + grid[i][j];
        }
        return vs[i][j];
    }

    List<String> rs = new ArrayList<>();
    public List<String> permutation(String s) {
        char[] sArray = s.toCharArray();
        Arrays.sort(sArray);
        List<Character> ls = new LinkedList<>();
        for (char c: sArray) {
            ls.add(c);
        }
        int n = s.length();
        StringBuffer sb = new StringBuffer();
        backtrace(ls, 0, n, sb);
        return rs;
    }

    public void backtrace(List<Character> ls, int idx, int n, StringBuffer sb) {
        if (idx == n) {
            rs.add(sb.toString());
            return;
        }
        for (int i=0; i<ls.size(); i++) {
            char c = ls.get(i);
            if (idx == 0 && i > 0 && ls.get(i-1) == c) continue;
            sb.append(c);
            ls.remove(i);
            backtrace(ls, idx+1, n, sb);
            sb.deleteCharAt(sb.length()-1);
            ls.add(i, c);
        }
    }

    public List<List<Integer>> pathSum(TreeNode root, int target) {

        List<List<Integer>> rs = new ArrayList<>();
        if (root == null) return rs;
        if (root.left != null || root.right != null) {
            List<List<Integer>> tmp1 = pathSum(root.left, target-root.val);
            List<List<Integer>> tmp2 = pathSum(root.right, target-root.val);
            tmp1.addAll(tmp2);
            if (tmp1.size() > 0) {
                for (List<Integer> ls: tmp1) {
                    ls.add(root.val);
                    rs.add(ls);
                }
            }
        } else {
            if (target == root.val) {
                List<Integer> ls = new ArrayList<>(root.val);
                System.out.println(root.val);
                rs.add(ls);
            }
        }
        return rs;
    }

    public int[] exchange(int[] nums) {
        int n = nums.length;
        int i = 0, j = n-1;
        while (i < j) {
            while (i < j && (nums[i] & 1) == 1) {
                i++;
            }
            while (i < j && (nums[j] & 1) == 0) {
                j--;
            }
            swap(nums, i, j);
            i++;
            j--;
        }
        PriorityQueue<Integer> pq = new PriorityQueue<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return 0;
            }
        });
        return nums;

    }

    public List<Integer> findDuplicates(int[] nums) {

        List<Integer> rs = new ArrayList<>();

        int n = nums.length, idx = 0;
        while (idx < n) {
            if (nums[idx] == idx + 1) {
                idx++;
            } else {
                int temp = nums[nums[idx]-1];
                if (temp == nums[idx]) {
                    rs.add(temp);
                    System.out.println(idx + "," + temp +","+ Arrays.toString(nums));
                    idx++;

                } else {
                    nums[nums[idx]-1] = nums[idx];
                    nums[idx] = temp;
                    System.out.println(idx+","+Arrays.toString(nums));
                }

            }
        }
        return rs;
    }

    public TreeNode recursiveBuildTree(int[] preorder, int[] inorder, int preL, int preR, int inL, int inR) {
        if (preL > preR) return null;
        int preRoot = preorder[preL];
        int idx = idxMap.get(preRoot);
        int lSize = idx - inL, rSize = inR - idx;
        TreeNode tn = new TreeNode(preRoot);
        tn.left = recursiveBuildTree(preorder, inorder, preL+1, preL+lSize, inL, idx-1);
        tn.right = recursiveBuildTree(preorder, inorder, preL+lSize+1, preR, idx+1, inR);
        return tn;
    }

    public TreeNode buildTree(int[] preorder, int[] inorder) {
        int n = preorder.length;
        for (int i = 0; i < n; i++) {
            idxMap.put(inorder[i], i);
        }
        return recursiveBuildTree(preorder, inorder, 0, n-1, 0, n-1);
    }

    public int findRepeatNumber(int[] nums) {
        int n = nums.length;
        int i = 0;
        while (i < n) {
            int v = nums[i];
            if (v != i) {
                if (v == nums[v]) {
                    return v;
                } else {
                    swap(nums, v, i);
                }
            } else {
                i++;
            }
        }
        return -1;
    }

    public int findTopKFrom(int[] arr1, int[] arr2, int k) {
        int l1 = arr1.length;
        int l2 = arr2.length;

        int idx1 = 0, idx2 = 0;
        while (true) {
            if (idx1 == l1) return arr2[idx2 + k -1];
            if (idx2 == l2) return arr1[idx1 + k -1];
            if (k == 1) return Math.min(arr1[idx1], arr2[idx2]);

            int var = k / 2;
            int nIdx1 = Math.min(idx1 + var, l1) - 1;
            int nIdx2 = Math.min(idx2 + var, l2) - 1;
            int p1 = arr1[nIdx1], p2 = arr2[nIdx2];
            if (p1 <= p2) {
                k -= (nIdx1 - idx1 + 1);
                idx1 = nIdx1 + 1;
            } else {
                k -= (nIdx2 - idx2 + 1);
                idx2 = nIdx2 + 1;
            }
        }

    }

    public int[] findDiagOrder(int[][] mat) {
        int m = mat.length, n = mat[0].length;
        int k = m * n;
        int[] rs = new int[k];

        int l = 0, i = 0, j = 0, prefix = 1;
        while (l < k) {
            while (i >= 0 && i < m && j >= 0 && j < n) {
                rs[l++] = mat[i][j];
//                System.out.println(mat[i][j]);
                i -= prefix;
                j += prefix;
            }
            i += prefix;
            j -= prefix;
            if (prefix > 0) {
                if (j + 1 < n) {
                    j++;
                } else {
                    i++;
                }
            } else {
                if (i + 1 < m) {
                    i++;
                } else {
                    j++;
                }
            }

            prefix *= -1;
        }
        return rs;
    }



    public void recursiveQuickSort(int[] arr, int left, int right) {
        if (left >= right) return;
        int base = arr[left];
        int i = left, j = right;

        while (i < j) {
            while (i < j && arr[j] >= base) {
                j--;
            }
            if (i < j) {
                arr[i] = arr[j];
                i++;
            }
            while (i < j && arr[i] <= base) {
                i++;
            }
            if (i < j) {
                arr[j] = arr[i];
                j--;
            }
        }
        arr[i] = base;
        recursiveQuickSort(arr, left, i-1);
        recursiveQuickSort(arr, i+1, right);
    }


    public int notRecursiveQuickSort(int[] arr, int left, int right) {
        if (left >= right) return left;

        int base = arr[left];
        int i = left, j = right;

        while (i < j) {
            while (i < j && arr[j] >= base) {
                j--;
            }
            if (i < j) {
                arr[i] = arr[j];
                i++;
            }
            while (i < j && arr[i] <= base) {
                i++;
            }
            if (i < j) {
                arr[j] = arr[i];
                j--;
            }
        }
        arr[i] = base;
        return i;
    }

    public void quickSort(int[] arr, int tp) {
        if (tp == 0) {
            recursiveQuickSort(arr, 0, arr.length-1);
        } else {
            Stack<Pair<Integer, Integer>> stack = new Stack<>();
            stack.push(new Pair<Integer, Integer>(0, arr.length-1));
            while (!stack.isEmpty()) {
                Pair<Integer, Integer> p = stack.pop();
                int mid = notRecursiveQuickSort(arr, p.getKey(), p.getValue());
                if (mid-1 > p.getKey()) stack.push(new Pair<Integer, Integer>(p.getKey(), mid-1));
                if (mid+1 < p.getValue()) stack.push(new Pair<Integer, Integer>(mid+1, p.getValue()));
            }
        }

    }




    public void heapSort(int[] arr) {
        int n = arr.length;
        int lastNotLeaf = n / 2 -1;

        for (int i=lastNotLeaf; i>=0; i--) {
            adjustHeap(arr, i, n-1);
        }
        for (int i=0; i<n; i++) {
            swap(arr, 0, n-i-1);
            adjustHeap(arr, 0, n-i-2);
        }
    }

    public void adjustHeap(int[] arr, int root, int end) {
        int base = arr[root];
        for (int i=root*2+1; i<=end; i=i*2+1) {
            if (i+1<=end && arr[i+1] > arr[i]) i += 1;
            if (arr[i] > base) {
                arr[root] = arr[i];
                root = i;
            } else {
                break;
            }
        }
        arr[root] = base;
    }

    public void mergeSort(int[] arr) {
        int n = arr.length;
        midMergeSort(arr, 0, n-1, new int[n]);
    }

    public void midMergeSort(int[] arr, int left, int right, int[] tmp) {
        if (left >= right) return;

        int mid = (left + right) / 2;
        midMergeSort(arr, left, mid, tmp);
        midMergeSort(arr, mid+1, right, tmp);

        int i = left, j = mid+1, c = 0;
        while (i <= mid && j<=right) {
            if (arr[i] <= arr[j]) {
                tmp[c] = arr[i];
                i++;
            } else {
                tmp[c] = arr[j];
                j++;
            }
            c++;
        }
        while (i <= mid) {
            tmp[c] = arr[i];
            i++;
            c++;
        }
        while (j <= right) {
            tmp[c] = arr[j];
            j++;
            c++;
        }
        for (int k=0; k< c; k++) {
            arr[left+k] = tmp[k];
        }
    }


    public void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }



}



//class Test {
//
//    public static void main(String[] args) {
//        NewArraySort nas = new NewArraySort();
//
//        int[] arr = new int[]{3,2,1,4};
////        nas.quickSort(arr, 0, 3);
////        nas.quickSort(arr, 1);
//        nas.mergeSort(arr);
//        Arrays.sort(a, new Comparator<int[]>() {
//
//            @Override
//            public int compare(int[] o1, int[] o2) {
//                return o1[0]-o2[0];
//            }
//        });
//        for (int i=0; i<arr.length; i++) {
//            System.out.println(arr[i]);
//        }
//    }
//}


class Solution {
    public static int coinChange(int[] coins, int amount) {
        if (amount ==0) return 0;
        int[] tmp = new int[amount+1];
        for (int i=1; i<=amount; i++) {
            tmp[i] = Integer.MAX_VALUE;;
        }
        tmp[0] = 0;
        int n = coins.length;
        for (int i=0; i<n; i++) {
            for (int j=coins[i]; j<=amount; j++) {
                if (tmp[j-coins[i]]+1 < tmp[j] && tmp[j-coins[i]] != Integer.MAX_VALUE) {
                    tmp[j] = tmp[j-coins[i]]+1;
                }

                System.out.println(tmp[j]);

            }
        }
        int rs = tmp[amount];
        return rs==Integer.MAX_VALUE?-1:rs;
    }


    public static int coinChange1(int[] coins, int amount) {
        if (amount ==0) return 0;
        int[] tmp = new int[amount+1];
        tmp[0] = 1;
        int n = coins.length;

        for (int i=0; i<n; i++) {
            for (int j=amount; j>=coins[i]; j--) {
                for (int k=1; k*coins[i]<=j; k++) {
                    if (tmp[j-k*coins[i]] > 0) tmp[j] += 1;
                }
            }
        }
        int rs = tmp[amount];
        return rs;
    }

    public static int findTargetSumWays(int[] nums, int target) {
        int n = nums.length;
        int s = 0;
        for (int i=0; i<n; i++) {
            s += nums[i];
        }
        int[] tmp0 = new int[2*s+1];
        tmp0[0+s] = 1;


        for (int i=0; i<n; i++) {
//            for (int p=0; p<2*s+1; p++) {
//                System.out.print(tmp0[p]);
//                System.out.print(" ");
//            }
//            System.out.println(" ");
            int[] next = new int[2*s+1];
            for (int j=-s; j<=s; j++) {

                int v = 0;
                if (j+nums[i]<=s && tmp0[j+nums[i]+s] > 0) v += tmp0[j+nums[i]+s];
                if (j-nums[i]>=-s && tmp0[j-nums[i]+s] > 0) v += tmp0[j-nums[i]+s];
//                if (i==1 && j==-1) {
//                    System.out.println(tmp0[j+nums[i]+s]);
//                    for (int p=0; p<2*s+1; p++) {
//                        System.out.print(tmp0[p]);
//                        System.out.print(" ");
//                    }
//                    System.out.println(" ");
//                    System.out.println(tmp0[j-nums[i]+s]);
//                }
                next[j+s] = v;

            }
            tmp0 = next;
        }
//        for (int j=0; j<2*s+1; j++) {
//            System.out.print(tmp0[j]);
//            System.out.print(" ");
//        }
//        System.out.println(" ");
        return target<=s ? tmp0[target+s]:-1;
    }


    public int numIslands(char[][] grid) {

        int rs = 0;
        int m = grid.length;
        int n = grid[0].length;

        Queue<Pair<Integer, Integer>> queue = new LinkedList<>();
        for (int i=0; i<m; i++) {
            for (int j=0; j<n; j++) {
                if (grid[i][j] == '1') {
                    rs += 1;
                    queue.add(new Pair<>(i, j));
                    while (! queue.isEmpty()) {
                        Pair<Integer, Integer> p = queue.poll();
                        int r = p.getKey(), t = p.getValue();
                        grid[r][t] = '0';
                        if (r-1>=0 && grid[r-1][t] == '1') queue.add(new Pair<>(r-1, t));
                        if (r+1<m && grid[r+1][t] == '1') queue.add(new Pair<>(r+1, t));
                        if (t-1>=0 && grid[r][t-1] == '1') queue.add(new Pair<>(r, t-1));
                        if (t+1<n && grid[r][t+1] == '1') queue.add(new Pair<>(r, t+1));
                    }
                }
            }
        }
        return rs;
    }


    public double findMedianSortedArrays(int[] nums1, int[] nums2) {
        int m = nums1.length, n = nums2.length;
        if ((m+n) % 2 == 0) {
            return (findMinK(nums1, nums2, (m+n)/2)+findMinK(nums1, nums2, (m+n)/2+1))/2;
        } else {
            return findMinK(nums1, nums2, (m+n)/2+1);
        }
    }

    public static int findMinK(int[] nums1, int[] nums2, int k) {
        int m = nums1.length, n = nums2.length;
        int begin1 = 0, begin2 = 0;
        while (k > 1) {
            int p = k / 2 -1;
            if (begin1 + p >= m) {
                int v = k - (m - begin1);
//                System.out.println(k);
//                System.out.println(m);
//                System.out.println(begin1);
                return nums2[begin2+v-1];
            }
            if (begin2 + p >= n) {
                int v = k - (n - begin2);
                return nums2[begin2+v-1];
            }
            if (nums1[begin1+p]<nums2[begin2+p]) {
                begin1 += (p+1);
            } else {
                begin2 += (p+1);
            }
            k -= (p+1);
        }
        int rs = 0;
        if (begin1 < m && begin2 < n) {
            rs = nums1[begin1]<nums2[begin2]? nums1[begin1]:nums2[begin2];
        } else if (begin1 < m) {
            rs = nums1[begin1];
        } else {
            rs = nums2[begin2];
        }
        return rs;
    }

    public static void main(String[] args) {
//        int[] arr = new int[]{1,1,1,1,1};
//        System.out.print(findTargetSumWays(arr, 3));
        int[] arr1 = new int[]{1,1,1,4,5};
        int[] arr2 = new int[]{2,3,4,5,6};

        String[][] board0 = new String[][]{{"5","3",".",".","7",".",".",".","."}, {"6",".",".","1","9","5",".",".","."},
                {".","9","8",".",".",".",".","6","."},{"8",".",".",".","6",".",".",".","3"},
                {"4",".",".","8",".","3",".",".","1"},{"7",".",".",".","2",".",".",".","6"},
                {".","6",".",".",".",".","2","8","."},{".",".",".","4","1","9",".",".","5"},{".",".",".",".","8",".",".","7","9"}
        };
        char[][] borad = new char[9][9];
        for (int i=0; i<9; i++) {
            for (int j=0; j<9; j++) {
                borad[i][j] = board0[i][j].charAt(0);
            }
        }

        Merge m = new Merge();
        TestCode tc = new TestCode();
        NewArraySort nas = new NewArraySort();
//        m.solveSudoku(borad);
//
//        for (int i=0; i<9; i++) {
//            for (int j=0; j<9; j++) {
//                System.out.print(borad[i][j] + " , ");
//            }
//            System.out.println();
//        }
//        System.out.println(m.convert("PAYPALISHIRING", 3));
//        System.out.println(tc.getLen("aabcdb"));
//        System.out.println(tc.maxProduct(new int[]{-2,0,-1}));
//        System.out.println(tc.minDistance("intention", "execution"));
//        System.out.println(tc.permute(new int[]{1,2,3}));
//        for (int k=0; k<9; k++) {
//            System.out.println(k);
//        }
//        System.out.println(tc.getPermutation(4, 9));
//        System.out.println(findMinK(arr1, arr2, 3));
//        System.out.println(nas.findDiagOrder(new int[][]{{1,2}, {3,4}}));
//        System.out.println(nas.findRepeatNumber(new int[]{2,3,1,0,2,5,3}));
//        System.out.println(nas.findDuplicates(new int[]{4,3,2,7,8,2,3,1}));
//        int[][] grid = new int[][]{{1,1,0,0},{1,1,0,0},{0,0,1,1}, {0,0,1,1}};
//        System.out.println(nas.maxAreaOfIsland(grid));
        String[] res = "0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0\\,0".split(";|,");

        Long[] arr = new Long[5];
        System.out.println(res[0]);
        for (int i = 0; i < res.length && i < 5; ++i) {
            arr[i] = Long.parseLong(res[i]);
        }

        Arrays.stream(arr).forEach(System.out::println);

    }
}

