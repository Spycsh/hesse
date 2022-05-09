package io.github.spycsh.hesse.benchmarks;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Applications need to filter the activities in [T0, Tt] to recover state
 * This function aims to receive and aggregate the processing time for filtering
 * it will output filtering time for the current query, the overall filtering time for handling different queries
 * the number of queries and the average filtering time one query takes
 */
public class BenchmarkFilterTimeFn implements StatefulFunction {
    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.benchmarks", "benchmark-filter-time");
    private static final ValueSpec<Long> OVERALL_TIME = ValueSpec.named("overallTime").withLongType();
    private static final ValueSpec<Long> RECORD_NUMBER = ValueSpec.named("recordNumber").withLongType();
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(BenchmarkFilterTimeFn::new)
            .withValueSpecs(OVERALL_TIME, RECORD_NUMBER)
            .build();

    private static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "filter-time");

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BenchmarkFilterTimeFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.isLong()){
            long time = message.asLong();
            long overallTime = context.storage().get(OVERALL_TIME).orElse(0L);
            overallTime += time;
            long recordNumber = context.storage().get(RECORD_NUMBER).orElse(0L);
            recordNumber += 1;

            double averageTimeForEachRecord = (double) overallTime / recordNumber;

            context.storage().set(OVERALL_TIME, overallTime);
            context.storage().set(RECORD_NUMBER, recordNumber);
            LOGGER.debug("[BenchmarkFilterTimeFn {}] current filtering time: {}, overall time in nano seconds: {}, record number: {}, average time for each filtering in nano seconds: {}",
                    context.self().id(), time, overallTime, recordNumber, String.format("%.2f", averageTimeForEachRecord));

            // egress current time to Kafka topic
            context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                    .withTopic("filter-time")
                    .withUtf8Key(String.valueOf(recordNumber))
                    .withUtf8Value(String.format(
                            "{\"time\":\"%s\", \"overall_time\":\"%s\", \"average_time\":\"%.1f\"}", time, overallTime, averageTimeForEachRecord))
                    .build());
        }

        return context.done();
    }
}
