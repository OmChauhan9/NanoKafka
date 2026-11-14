package com.simplekafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

class BrokerShutdownTest {

    // The hang needs a connection accepted in the instant the broker stops, so the test stops many brokers
    // while several clients keep connecting.
    @Test
    void stoppingABrokerReleasesEveryClientWaitingOnIt() throws Exception {
        int trials = 60;
        int clientsPerTrial = 4;
        int hung = 0;
        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            for (int trial = 0; trial < trials; trial++) {
                TestCluster cluster = TestCluster.start(1);
                cluster.client(1).createTopic("t", 1, (short) 1);
                CountDownLatch sending = new CountDownLatch(clientsPerTrial);
                List<Future<?>> clients = new ArrayList<>();
                for (int c = 0; c < clientsPerTrial; c++) {
                    clients.add(pool.submit(() -> {
                        sending.countDown();
                        while (true) {
                            try {
                                Protocol.call("localhost", cluster.port(1), Protocol.encodeFetchRequest("t", 0, 0, 1024));
                            } catch (Exception e) {
                                return null;
                            }
                        }
                    }));
                }
                sending.await();
                Thread.sleep(trial % 7);
                cluster.close();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                for (Future<?> client : clients) {
                    try {
                        client.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    } catch (TimeoutException e) {
                        hung++;
                        client.cancel(true);
                    }
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, hung, "clients still blocked 3 s after their broker stopped");
    }
}
