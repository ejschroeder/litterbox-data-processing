package lol.schroeder;

import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@RequiredArgsConstructor
public class ValidLitterboxEventFilter extends ProcessFunction<LitterBoxEvent, LitterBoxEvent> {

    private final OutputTag<LitterBoxEvent> invalidEventOutputTag;

    @Override
    public void processElement(LitterBoxEvent event, ProcessFunction<LitterBoxEvent, LitterBoxEvent>.Context ctx, Collector<LitterBoxEvent> out) throws Exception {
        if (isLitterboxEventValid(event)) {
            out.collect(event);
        } else if (invalidEventOutputTag != null) {
            ctx.output(invalidEventOutputTag, event);
        }
    }

    private boolean isLitterboxEventValid(LitterBoxEvent event) {
        return event.getCatWeight() > 0.25;
    }
}
