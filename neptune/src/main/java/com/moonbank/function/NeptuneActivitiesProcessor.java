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


import com.google.cloud.functions.CloudEventsFunction;
import com.google.gson.Gson;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;


@Slf4j
public class NeptuneActivitiesProcessor implements CloudEventsFunction {

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
        String decodedData =
                new String(Base64.getDecoder().decode(encodedData), StandardCharsets.UTF_8);
        log.info("Hello, " + decodedData + "!");

    }
}