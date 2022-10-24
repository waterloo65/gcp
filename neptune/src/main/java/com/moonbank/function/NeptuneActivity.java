package com.moonbank.pipeline;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.math.BigDecimal;

@Builder
@Required
@DefaultSchema(JavaFieldSchema.class)
@EqualsAndHashCode
public class Activity {

    String timestamp;
    String ipAddr;
    String action;
    String srcAccount;
    String destAccount;
    BigDecimal amount;
    String customerName;

}
