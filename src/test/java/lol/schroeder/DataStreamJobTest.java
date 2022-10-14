package lol.schroeder;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataStreamJobTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void test() {
        CollectSink collectSink = new CollectSink();
        CollectSink invalidEventSink = new CollectSink();
        new DataStreamJob(new TestSource(), collectSink, invalidEventSink).execute();
        System.out.println(collectSink.getValues());
    }

    private static class CollectSink implements SinkFunction<LitterBoxEvent> {

        // must be static
        public static final List<LitterBoxEvent> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(LitterBoxEvent value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }

        public List<LitterBoxEvent> getValues() {
            return values;
        }
    }
}
