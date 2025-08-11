package com.github.kagkarlsson.scheduler.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TestUtils {

    public static Instant truncateInstant(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MILLIS);
    }

}
