package com.moonbank.function;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NeptuneActivityTest {

    @Test
    public void testConvertCsv2Activity() {

        var expected = NeptuneActivity.builder()
                .id("20200812040801981475")
                .ipAddr("195.174.170.81")
                .action("UPDATE")
                .accountNo("GB25BZMX47593824219489")
                .actionId(Integer.valueOf("4"))
                .name("Emily Blair")
                .actionby("STAFF")
                .build();
        var actual = NeptuneActivity.fromCsv("20200812040801981475,195.174.170.81,UPDATE," +
                "GB25BZMX47593824219489,4,Emily Blair,STAFF");

        assertEquals(expected, actual);

    }

}
