package com.moonbank.function;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;

import java.math.BigDecimal;

@Builder
@DefaultSchema(JavaFieldSchema.class)
@EqualsAndHashCode
public class MarsActivity {

    String timestamp;
    String ipAddr;
    String action;
    String srcAccount;
    String destAccount;
    BigDecimal amount;
    String customerName;

    public static MarsActivity fromCsv(String input) {
        var output = input.split(",");
        return MarsActivity.builder()
                .timestamp(output[0])
                .ipAddr(output[1])
                .action(output[2])
                .srcAccount(output[3])
                .destAccount(output[4])
                .amount(new BigDecimal(output[5]))
                .customerName(output[6])
                .build();

    }
}
