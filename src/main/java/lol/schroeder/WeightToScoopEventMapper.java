package lol.schroeder;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class WeightToScoopEventMapper extends KeyedProcessFunction<String, ScaleWeightEvent, ScoopEvent> {

    private ValueState<WindowedRunningStats> windowedRunningStats;
    private ValueState<Boolean> isScooping;
    private ValueState<IntermediateData> intermediateData;
    private static final double STD_DEV_THRESHOLD = 0.001;
    private static final double MINIMUM_SCOOP_WEIGHT = 0.1;

    @Override
    public void open(Configuration parameters) {
        windowedRunningStats = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("running-stats", WindowedRunningStats.class));

        intermediateData = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("intermediate-data", IntermediateData.class));

        isScooping = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("is-scooping", Boolean.class));
    }

    @Override
    public void processElement(ScaleWeightEvent event, KeyedProcessFunction<String, ScaleWeightEvent, ScoopEvent>.Context ctx, Collector<ScoopEvent> out) throws Exception {
        setupDefaultState();

        if (isScooping.value()) {
            handleScoopingState(event, ctx, out);
        } else {
            handleStandbyState(event);
        }
    }

    @SneakyThrows
    private void handleStandbyState(ScaleWeightEvent event) {
        WindowedRunningStats wrs = updateRunningStats(event);

        if (wrs.isBufferFull() && wrs.getSampleStandardDeviation() > STD_DEV_THRESHOLD) {
            IntermediateData data = new IntermediateData();
            data.setStandbyMean(wrs.getMean());
            data.setStartTimestamp(event.getTime());
            intermediateData.update(data);
            isScooping.update(true);
        }
    }

    @SneakyThrows
    private void handleScoopingState(ScaleWeightEvent event, KeyedProcessFunction<String, ScaleWeightEvent, ScoopEvent>.Context ctx, Collector<ScoopEvent> out) {
        WindowedRunningStats wrs = updateRunningStats(event);

        if (wrs.getSampleStandardDeviation() > STD_DEV_THRESHOLD) {
            return;
        }

        IntermediateData data = intermediateData.value();
        double scoopedWeight = data.getStandbyMean() - wrs.getMean();

        if (scoopedWeight > MINIMUM_SCOOP_WEIGHT) {
            Instant startInstant = Instant.ofEpochSecond(data.getStartTimestamp());
            Instant endInstant = Instant.ofEpochSecond(event.getTime());

            out.collect(ScoopEvent.builder()
                    .deviceId(ctx.getCurrentKey())
                    .startTime(startInstant)
                    .endTime(endInstant)
                    .scoopedWeight(scoopedWeight)
                    .build());
        }

        isScooping.update(false);
        intermediateData.clear();
    }

    @SneakyThrows
    private WindowedRunningStats updateRunningStats(ScaleWeightEvent event) {
        WindowedRunningStats runningStats = windowedRunningStats.value();
        runningStats.add(event.getValue());
        windowedRunningStats.update(runningStats);
        return runningStats;
    }

    @SneakyThrows
    private void setupDefaultState() {
        if (windowedRunningStats.value() == null) {
            windowedRunningStats.update(new WindowedRunningStats(20));
        }

        if (isScooping.value() == null) {
            isScooping.update(false);
        }
    }

    @Data
    static class IntermediateData {
        private long startTimestamp;
        private double standbyMean;
    }
}
