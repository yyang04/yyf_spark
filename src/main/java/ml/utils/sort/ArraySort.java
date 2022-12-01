package ml.utils.sort;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * author: renjian
 * Datetime: 2021/1/13 上午11:16
 * email: renjian04@meituan.com
 * Description:
 */
public class ArraySort {

    public static void main(String[] args) {
//        int[] arr = new int[]{10,2,1,3,4,9,7,8,5,};
////        bubbleSort(arr);
////        quickSort(arr, 0, arr.length - 1);
//
////        mergeSort(arr, 0, arr.length - 1);
//        heapSortNew(arr);
////        insertSort(arr);
////        shellSort(arr);
////        selectSort(arr);
//        for (int i=0; i<arr.length; i++) {
//            System.out.println(arr[i]);
//
//        }
        PriorityQueue<TreeNode> pq = new PriorityQueue<>((n1, n2) -> n1.val - n2.val);
        int[] a = new int[]{1,2,3};
        Arrays.asList(1,2,3).stream().filter(x -> x > 2).map(x -> x+5).collect(Collectors.toList()).forEach(System.out::println);
    }

    public static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void heapSortNew(int[] arr) {
        int n = arr.length;
        int lastNotLeaf = n / 2 - 1;

        for (int i = lastNotLeaf; i >=0 ; i--) {
            adjustMaxHeap(arr, i, n-1);
        }
        for (int i = n-1; i > 0; i--) {
            swap(arr, 0, i);
            adjustMaxHeap(arr, 0, i-1);
        }
    }


    public static void adjustMaxHeap(int[] arr, int l, int r) {
        int temp = arr[l];
        for (int i = l*2+1; i <= r; i = i * 2 + 1) {
            if (i < r && arr[i+1] > arr[i]) i++;
            if (arr[i] > temp) {
                arr[l] = arr[i];
                l = i;
            } else {
                break;
            }
        }
        arr[l] = temp;
    }

    /**
     * 冒泡排序
     * @param arr
     */
    public static void bubbleSort(int[] arr) {
        int n = arr.length;
        for (int i=n-1; i>0; i--) {
            for (int j=0; j<i; j++) {
                if (arr[j+1] < arr[j]) {
                    int tmp = arr[j+1];
                    arr[j+1] = arr[j];
                    arr[j] = tmp;
                }
            }
        }
    }


    /**
     * 选择排序
     * @param arr
     */
    public static void selectSort(int[] arr) {
        int n = arr.length;
        for (int i=n-1; i>0; i--) {
            for (int j=0; j<i; j++) {
                if (arr[j] > arr[i]) {
                    int tmp = arr[i];
                    arr[i] = arr[j];
                    arr[j] = tmp;
                }
            }
        }
    }


    /**
     * 快排
     * @param arr
     * @param left
     * @param right
     */
    public static void quickSort(int[] arr, int left, int right) {
        if (left >= right) return;
        int pivot = quickSortPartition(arr, left, right);
        quickSort(arr, left, pivot-1);
        quickSort(arr, pivot+1, right);
    }

    public static void unRecursiveQuickSort(int[] arr, int left, int right) {
        Stack<Integer> stack = new Stack<>();
        stack.push(left);
        stack.push(right);
        while (!stack.isEmpty()) {
            int end = stack.pop(), begin = stack.pop();
            if (begin >= end) continue;
            int pivot = quickSortPartition(arr, begin, end);
            if (begin < pivot-1) {
                stack.push(begin);
                stack.push(pivot-1);
            }
            if (pivot+1<end) {
                stack.push(pivot+1);
                stack.push(end);
            }
        }
    }


    public static int quickSortPartition(int[] arr, int left, int right) {
        int mid = arr[left];
        int i = left;
        int j = right;
        while (i < j) {
            while (i < j && arr[j] >= mid) {
                j--;
            }
            if (i < j) {
                arr[i] = arr[j];
                i += 1;
            }
            while (i < j && arr[i] <= mid) {
                i++;
            }
            if (i < j) {
                arr[j] = arr[i];
                j -= 1;
            }
        }
        arr[i] = mid;
        return i;
    }


    /**
     * 归并排序
     * @param arr
     * @param left
     * @param right
     */
    public static void mergeSort(int[] arr, int left, int right) {
        int n = arr.length;
        int[] aux = new int[n];
        midMergeSort(arr, aux, left, right);
    }

    public static void midMergeSort(int[] arr, int[] aux, int left, int right) {
        if (left == right) return;
        int mid = (left + right) / 2;

        midMergeSort(arr, aux, left, mid);
        midMergeSort(arr, aux, mid+1, right);

        int i = left, j = mid+1, idx = left;
        while (i <= mid || j <= right) {
            if (i > mid) {
                while (j <= right) aux[idx++] = arr[j++];
                continue;
            }
            if (j > right) {
                while (i <= mid) aux[idx++] = arr[i++];
                continue;
            }
            if (arr[i] <= arr[j]) {
                aux[idx++] = arr[i++];
            } else {
                aux[idx++] = arr[j++];
            }
        }
        for (int m=left; m<=right; m++) {
            arr[m] = aux[m];
        }
    }


    /**
     * 堆排序
     * @param arr
     */
    public static void heapSort(int[] arr) {
        int n = arr.length;
        int lastNotLeaf = n / 2 - 1;
        for (int i=lastNotLeaf; i>=0; i--) {
            maxHeap(arr, i, n);
        }

        for (int i=n-1; i>0; i--) {
            int tmp = arr[i];
            arr[i] = arr[0];
            arr[0] = tmp;
            maxHeap(arr, 0, i);
        }
    }

    public static void maxHeap(int[] arr, int left, int right) {

        int tmp = arr[left];

        for (int i=left*2+1; i<right; i=i*2+1) {
            if (i+1 < right && arr[i] < arr[i+1]) {
                i += 1;
            }
            if (arr[i] > tmp) {
                arr[left] = arr[i];
                left = i;
            } else {
                break;
            }
        }
        arr[left] = tmp;
    }


    /**
     * 插入排序
     * @param arr
     */
    public static void insertSort(int[] arr) {
        shellSort(arr, 1);
    }

    public static void shellSort(int[] arr) {
        int initGap = arr.length / 2;
        shellSort(arr, initGap);
    }

    public static void shellSort(int[] arr, int initGap) {
        int n = arr.length;
        for (int gap=initGap; gap>0; gap/=2) {
            int idx, value;
            for (int i=gap; i<n; i++) {
                idx = i - gap;
                value = arr[i];
                while (idx >= 0 &&  value < arr[idx]) {
                    arr[idx+gap] = arr[idx];
                    idx -= gap;
                }
                arr[idx+gap] = value;
            }
        }
    }





}
