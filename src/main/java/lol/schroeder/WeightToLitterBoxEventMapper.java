package lol.schroeder;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;


@Slf4j
public class WeightToLitterBoxEventMapper extends KeyedProcessFunction<String, ScaleWeightEvent, LitterBoxEvent> {

    private ValueState<WindowedRunningStats> windowedRunningStats;
    private ValueState<State> currentState;
    private ValueState<IntermediateData> intermediateData;
    private static final double STANDBY_STD_DEV_THRESHOLD = 0.002;
    private static final double IN_BOX_STD_DEV_THRESHOLD = 0.025;
    private static final double STANDARD_DEVIATION_THRESHOLD = 0.002;
    private static final double Z_SCORE_THRESHOLD = 8;
    private static final long WATCHDOG_TIMER_MILLIS = 5 * 60 * 1000;

    @Override
    public void open(Configuration parameters) {
        windowedRunningStats = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("running-stats", WindowedRunningStats.class));

        currentState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("current-state", State.class));

        intermediateData = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("intermediate-data", IntermediateData.class));
    }


    @Override
    public void processElement(ScaleWeightEvent value, KeyedProcessFunction<String, ScaleWeightEvent, LitterBoxEvent>.Context ctx, Collector<LitterBoxEvent> out) throws Exception {
        setupDefaultState();

        State state = currentState.value();
        switch(state) {
            case STANDBY:
                handleStandbyState(value, ctx);
                break;
            case STEPPING_IN:
                handleSteppingInState();
                break;
            case IN_BOX:
                handleInBoxState(value);
                break;
            case STEPPING_OUT:
                handleSteppingOutState(value, out, ctx);
                break;
            default:
                log.error("Unknown state encountered: {}", state);
        }

        WindowedRunningStats runningStats = windowedRunningStats.value();
        runningStats.add(value.getValue());
        windowedRunningStats.update(runningStats);
    }

    @SneakyThrows
    private void setupDefaultState() {
        if (windowedRunningStats.value() == null) {
            windowedRunningStats.update(new WindowedRunningStats(10));
        }

        if (currentState.value() == null) {
            currentState.update(State.STANDBY);
        }
    }

    @SneakyThrows
    private void handleStandbyState(ScaleWeightEvent event, KeyedProcessFunction<String, ScaleWeightEvent, LitterBoxEvent>.Context ctx) {
        WindowedRunningStats wrs = windowedRunningStats.value();
        if (wrs.isBufferFull() && wrs.getSampleZScore(event.getValue()) > Z_SCORE_THRESHOLD) {
            log.info("Moving from Standby to Stepping in. mean={} stdDev={} value={}", wrs.getMean(), wrs.getSampleStandardDeviation(), event.getValue());
            IntermediateData data = new IntermediateData();
            data.setStandbyMean(wrs.getMean());
            data.setStartTimestamp(event.getTime());
            data.setWatchdogTimestampMillis((event.getTime() * 1000) + WATCHDOG_TIMER_MILLIS);
            log.info("Setting watchdog timer for time={}", data.getWatchdogTimestampMillis());
            ctx.timerService().registerEventTimeTimer(data.getWatchdogTimestampMillis());

            intermediateData.update(data);
            currentState.update(State.STEPPING_IN);
        }
    }

    @SneakyThrows
    private void handleSteppingInState() {
        WindowedRunningStats wrs = windowedRunningStats.value();
        if (wrs.getSampleStandardDeviation() <= IN_BOX_STD_DEV_THRESHOLD) {
            log.info("Moving from Stepping In to In Box mean={} stdDev={}", wrs.getMean(), wrs.getSampleStandardDeviation());
            IntermediateData data = intermediateData.value();
            data.setInBoxMean(wrs.getMean());
            intermediateData.update(data);
            currentState.update(State.IN_BOX);
        }
    }

    @SneakyThrows
    private void handleInBoxState(ScaleWeightEvent event) {
        WindowedRunningStats wrs = windowedRunningStats.value();
        if (wrs.getSampleZScore(event.getValue()) < -Z_SCORE_THRESHOLD) {
            log.info("Moving from In Box to Stepping Out. mean={} stdDev={} value={}", wrs.getMean(), wrs.getSampleStandardDeviation(), event.getValue());
            currentState.update(State.STEPPING_OUT);
        }
    }

    @SneakyThrows
    private void handleSteppingOutState(ScaleWeightEvent event, Collector<LitterBoxEvent> out, KeyedProcessFunction<String, ScaleWeightEvent, LitterBoxEvent>.Context ctx) {
        WindowedRunningStats wrs = windowedRunningStats.value();
        if (wrs.getSampleStandardDeviation() <= STANDBY_STD_DEV_THRESHOLD) {
            log.info("Moving from Stepping Out to Standby. mean={} stdDev={}", wrs.getMean(), wrs.getSampleStandardDeviation());
            IntermediateData data = intermediateData.value();
            double currentMeanWeight = wrs.getMean();

            Instant start = Instant.ofEpochSecond(data.getStartTimestamp());
            Instant end = Instant.ofEpochSecond(event.getTime());

            out.collect(LitterBoxEvent.builder()
                    .deviceId(ctx.getCurrentKey())
                    .startTime(start)
                    .endTime(end)
                    .catWeight(data.getInBoxMean() - currentMeanWeight)
                    .eliminationWeight(currentMeanWeight - data.getStandbyMean())
                    .build());

            ctx.timerService().deleteEventTimeTimer(data.getWatchdogTimestampMillis());
            currentState.update(State.STANDBY);
            intermediateData.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, ScaleWeightEvent, LitterBoxEvent>.OnTimerContext ctx, Collector<LitterBoxEvent> out) throws Exception {
        log.info("Watchdog timer called. Resetting process state. timestamp={}", timestamp);
        currentState.update(State.STANDBY);
        intermediateData.clear();
    }

    enum State {
        STANDBY,
        STEPPING_IN,
        IN_BOX,
        STEPPING_OUT
    }

    @Data
    static class IntermediateData {
        private long startTimestamp;
        private double standbyMean;
        private double inBoxMean;
        private long watchdogTimestampMillis;
    }
}
