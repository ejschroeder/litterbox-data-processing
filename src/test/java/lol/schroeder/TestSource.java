package lol.schroeder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public class TestSource implements SourceFunction<ScaleWeightEvent> {

    private volatile boolean running = true;
    private static final int COUNT = 100;

    private List<CSVRecord> csvRecords;

    public TestSource() {
        try (InputStream in = getClass().getResourceAsStream("/test-event-combined.csv")) {
            csvRecords = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(new InputStreamReader(in))
                    .getRecords();

            csvRecords.sort(Comparator.comparing(csvRecord -> csvRecord.get(2)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(SourceContext<ScaleWeightEvent> ctx) throws Exception {
        for (CSVRecord rec : csvRecords) {
            double sample = Double.parseDouble(rec.get(1));
            String timestamp = rec.get(2);

            Instant instant = Instant.parse(timestamp);

            ctx.collect(ScaleWeightEvent.builder()
                    .deviceId("scale-1")
                    .sensor("load_cell")
                    .value(sample)
                    .time(instant.getEpochSecond())
                    .build());
        }
    }

    @Override
    public void cancel() {
        // Do nothing
    }
}
