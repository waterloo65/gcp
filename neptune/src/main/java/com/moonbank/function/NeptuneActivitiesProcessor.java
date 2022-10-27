/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moonbank.function;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.functions.CloudEventsFunction;
import com.google.gson.Gson;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class NeptuneActivitiesProcessor implements CloudEventsFunction {

    private final BigQuery bigQuery;
    private final TableId rawTableId;
    private final TableId acitvitiesTableId;


    public NeptuneActivitiesProcessor() {

        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        bigQuery = BigQueryOptions.getDefaultInstance().getService();
        rawTableId = TableId.of("neptune", "raw");
        acitvitiesTableId = TableId.of("neptune", "activities");

    }

    @Override
    public void accept(CloudEvent event) {
        // The Pub/Sub message is passed as the CloudEvent's data payload.
        if (event.getData() == null) {
            log.warn("Event data is null {}", event);
        }

        // Extract Cloud Event data and convert to PubSubBody
        String cloudEventData = new String(event.getData().toBytes(), StandardCharsets.UTF_8);

        Gson gson = new Gson();
        PubSubBody body = gson.fromJson(cloudEventData, PubSubBody.class);
        // Retrieve and decode PubSub message data
        String encodedData = body.getMessage().getData();
        String payload =
                new String(Base64.getDecoder().decode(encodedData), StandardCharsets.UTF_8);

        log.info("payload={}", payload);

        insertToBigQuery(payload);

        log.info("Successfully insert to biqquery");

    }

    private void insertToBigQuery(String csv) {
        try {
            // -- insert raw message
            var response =
                    bigQuery.insertAll(
                            InsertAllRequest.newBuilder(rawTableId)
                                    .addRow(Map.of("message", csv))
                                    .build());

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    log.error("Response error: {}", entry.getValue());
                }
                throw new RuntimeException("Fail to insert raw record");
            }


            log.info("Inserted raw message");

            // -- insert activity message
            var activity = NeptuneActivity.fromCsv(csv);

            /*
             * SCHEMA:
             * id:string
             * ipaddress:string
             * action:string
             * accountnumber:string
             * actionid:integer
             * name:string
             * actionby:string
             */
            // Create rows to insert
            Map<String, Object> row = new HashMap<>();
            row.put("id", activity.id);
            row.put("ipaddress", activity.ipAddr);
            row.put("action", activity.action);
            row.put("accountnumber", activity.accountNo);
            row.put("actionid", activity.actionId);
            row.put("name", activity.name);
            row.put("actionby", activity.actionby);

            response =
                    bigQuery.insertAll(
                            InsertAllRequest.newBuilder(acitvitiesTableId)
                                    .addRow(row)
                                    .build());

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    log.error("Response error: {}", entry.getValue());
                }
                throw new RuntimeException("Fail to insert actvity record");
            }

        } catch (BigQueryException e) {
            log.error("Insert operation not performed", e);
        }
    }
}