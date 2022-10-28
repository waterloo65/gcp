package com.moonbank.function;

import com.google.api.client.util.StringUtils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Builder
@EqualsAndHashCode
@Slf4j
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
        Integer actionId = null;
        try {
            actionId = Integer.parseInt(output[4]);
        }
        catch (Exception e) {
            log.error("Fail to convert {}", output[4]);
        }

        return NeptuneActivity.builder()
                .id(output[0])
                .ipAddr(output[1])
                .action(output[2])
                .accountNo(output[3])
                .actionId(actionId)
                .name(output[5])
                .actionby(output[6])
                .build();

    }
}
