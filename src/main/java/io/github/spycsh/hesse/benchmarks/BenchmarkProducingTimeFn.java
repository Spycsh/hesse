package io.github.spycsh.hesse.benchmarks;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class BenchmarkProducingTimeFn implements StatefulFunction {
    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.benchmarks", "benchmark-producing-time");
    private static final ValueSpec<Long> OVERALL_TIME = ValueSpec.named("overallTime").withLongType();
    private static final ValueSpec<Long> RECORD_NUMBER = ValueSpec.named("recordNumber").withLongType();
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(BenchmarkProducingTimeFn::new)
            .withValueSpecs(OVERALL_TIME, RECORD_NUMBER)
            .build();

    private static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "producing-time");

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BenchmarkProducingTimeFn.class);

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
            LOGGER.trace("[BenchmarkProducingTimeFn {}] current producing time: {}, overall time in nano seconds: {}, record number: {}, average time for each producing in nano seconds: {}",
                    context.self().id(), time, overallTime, recordNumber, String.format("%.2f", averageTimeForEachRecord));

            // egress current time to Kafka topic
            context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                    .withTopic("producing-time")
                    .withUtf8Key(String.valueOf(recordNumber))
                    .withUtf8Value(String.format(
                            "{\"time\":\"%s\", \"overall_time\":\"%s\", \"average_time\":\"%.1f\"}", time, overallTime, averageTimeForEachRecord))
                    .build());
        }

        return context.done();
    }
}
