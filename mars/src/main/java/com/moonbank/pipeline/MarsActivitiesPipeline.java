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

package com.moonbank.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;


public class MarsActivitiesPipeline {

    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MarsActivitiesPipeline.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Options extends DataflowPipelineOptions {
        @Description("Pubsub topic")
        String getInputTopic();
        void setInputTopic(String s);

        @Description("Window duration length, in seconds")
        Integer getWindowDuration();
        void setWindowDuration(Integer windowDuration);

        @Description("BigQuery activities table")
        String getOutputTable();
        void setOutputTable(String s);

        @Description("BigQuery raw table")
        String getRawTable();
        void setRawTable(String s);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the {@link MarsActivitiesPipeline#run(Options)} method to
     * start the pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }



    /**
     * Runs the pipeline to completion with the specified options. This method does
     * not wait until the pipeline is finished before returning. Invoke
     * {@code result.waitUntilFinish()} on the result object to block until the
     * pipeline is finished running if blocking programmatic execution is required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("mars-activities-" + System.currentTimeMillis());

        /*
         * Steps:
         * 1) Read something
         * 2) Transform something
         * 3) Write something
         */
        PCollection<Activity> logs = pipeline
                // Read in lines from GCS and Parse to Activities
                .apply("ReadMessage", PubsubIO.readStrings()
                    .withTimestampAttribute("timestamp")
                    .fromTopic(options.getInputTopic()))
                .apply("ParseCsv", ParDo.of( new DoFn<String, Activity>() {
                         @ProcessElement
                         public void processElement(@Element String csv, OutputReceiver<Activity> r) {
                             var activity = convertCsv2Activity(csv);
                            r.output(activity);
                         }
                     }
                ));

        // Write the activity to BigQuery
        logs.apply("WindowByMinute", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration()))))
            .apply("WriteToBQ",
                        BigQueryIO.<Activity>write().to(options.getOutputTable()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        // Write the raw logs to BigQuery
        logs.apply("WriteRawToBQ",
                        BigQueryIO.<Activity>write().to(options.getRawTable()).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        LOG.info("Building pipeline...");

        return pipeline.run();
    }

    public static Activity convertCsv2Activity(String input) {
        var output = input.split(",");
        return Activity.builder()
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