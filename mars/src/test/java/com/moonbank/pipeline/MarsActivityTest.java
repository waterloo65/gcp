package com.moonbank.pipeline;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MarsActivityTest {

    @Test
    public void testConvertCsv2Activity() {

        var expected = MarsActivity.builder()
                .timestamp("20221016000006358410")
                .ipAddr("203.172.77.210")
                .action("SCHEDULE")
                .srcAccount("GB63IOEE41708907941843")
                .destAccount("GB65TBIT49649150943375")
                .amount(new BigDecimal("691.42"))
                .customerName("Sara Anderson")
                .build();
        var actual = MarsActivity.fromCsv(
                "20221016000006358410,203.172.77.210," +
                "SCHEDULE,GB63IOEE41708907941843,GB65TBIT49649150943375,691.42," +
                "Sara Anderson");

        assertEquals(expected, actual);
    }

}
