package com.github.dataswitch.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

public class QueuePerfTest {
    static ExecutorService e = Executors.newFixedThreadPool(10);
    static int N = 1000000;

    public static void main(String[] args) throws Exception {    
        for (int i = 0; i < 10; i++) {
            int length = (i == 0) ? 1 : i * 5;
            System.out.print("length:"+length + "\t tps: \t");
            System.out.print(doTest(new LinkedBlockingQueue<Integer>(length), N) + "\t");
            System.out.print(doTest(new ArrayBlockingQueue<Integer>(length), N) + "\t");
            System.out.print(doTest(new SynchronousQueue<Integer>(), N));
            System.out.println();
        }

        e.shutdown();
    }

    private static long doTest(final BlockingQueue<Integer> q, final int n) throws Exception {
        long startTime = System.nanoTime();

        e.submit(new Runnable() {
            public void run() {
                for (int i = 0; i < n; i++)
                    try { q.put(i); } catch (InterruptedException ex) {}
            }
        });    

        Long r = e.submit(new Callable<Long>() {
            public Long call() {
                long sum = 0;
                for (int i = 0; i < n; i++)
                    try { sum += q.take(); } catch (InterruptedException ex) {}
                return sum;
            }
        }).get();
        long costTime = System.nanoTime() - startTime;

        long tps = (long)(1000000000.0 * N / costTime); // Throughput, items/sec
        return tps;
    }
}  