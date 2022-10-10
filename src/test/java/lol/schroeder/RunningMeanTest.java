package lol.schroeder;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class RunningMeanTest {

    private List<CSVRecord> csvRecords;

    @Before
    public void setup() throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/test-event-missed-1.csv")) {
            csvRecords = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(new InputStreamReader(in))
                    .getRecords();
        }
    }

    @Test
    public void windowTesting() {
        List<StatContainer> statContainers = new ArrayList<>();

        csvRecords.sort(Comparator.comparing(csvRecord -> csvRecord.get(2)));

        WindowedRunningStats wrm = new WindowedRunningStats(10);
        for (CSVRecord csvRecord : csvRecords) {
            double sample = Double.parseDouble(csvRecord.get(1));
            String timestamp = csvRecord.get(2);
            double zScore = wrm.getSampleZScore(sample);

            wrm.add(sample);

            statContainers.add(StatContainer.builder()
                    .timestamp(timestamp)
                    .sample(sample)
                    .mean(wrm.getMean())
                    .variance(wrm.getSampleVariance())
                    .standardDeviation(wrm.getSampleStandardDeviation())
                    .zScore(zScore)
                    .build());
        }

        try (CSVPrinter printer = new CSVPrinter(new FileWriter("out-windowed-10-test-event-missed-1.csv"), CSVFormat.DEFAULT)) {
            printer.printRecord("timestamp", "sample", "mean", "sampleVariance", "sampleStandardDeviation", "zScore");

            for (int i = 0; i < statContainers.size() - 1; i++) {
                StatContainer container = statContainers.get(i);

                printer.printRecord(
                        container.getTimestamp(),
                        container.getSample(),
                        container.getMean(),
                        container.getVariance(),
                        container.getStandardDeviation(),
                        container.getZScore());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Data
    @Builder
    static class StatContainer {
        private double sample;
        private String timestamp;
        private double zScore;
        private double mean;
        private double variance;
        private double standardDeviation;
    }


    enum State {
        STANDBY,
        IN_BOX,
        STEPPING_IN,
        STEPPING_OUT
    }
}