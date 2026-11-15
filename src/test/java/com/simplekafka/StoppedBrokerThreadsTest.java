package com.simplekafka;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class StoppedBrokerThreadsTest {

    @Test
    void aStoppedBrokerStopsRetryingControllerElection() throws Exception {
        long before = electionThreads();
        for (int i = 0; i < 3; i++) {
            TestCluster.start(1).close();
        }

        TestCluster.await("election retries from stopped brokers to end", () -> electionThreads() <= before);
    }

    private static long electionThreads() {
        return Thread.getAllStackTraces().values().stream()
                .filter(stack -> Arrays.stream(stack).anyMatch(frame -> frame.getMethodName().contains("electController")))
                .count();
    }
}
