package com.moonbank.function;

import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder
@EqualsAndHashCode
public class NeptuneActivity {

    String id;
    String ipAddr;
    String action;
    String accountNo;
    Integer actionId;
    String name;
    String actionby;

    public static NeptuneActivity fromCsv(String input) {
        var output = input.split(",");
        return NeptuneActivity.builder()
                .id(output[0])
                .ipAddr(output[1])
                .action(output[2])
                .accountNo(output[3])
                .actionId(Integer.valueOf(output[4]))
                .name(output[5])
                .actionby(output[6])
                .build();

    }
}
