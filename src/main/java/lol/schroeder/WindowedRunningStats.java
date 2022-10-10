package lol.schroeder;

import java.util.Optional;

public class WindowedRunningStats {
    private final CircularQueue<Double> circularQueue;
    private double mean;
    private double dSquared;

    public WindowedRunningStats(int size) {
        this.circularQueue = new CircularQueue<>(size);
        this.mean = 0;
        this.dSquared = 0;
    }

    public void add(double sample) {
        Optional<Double> poppedValue = circularQueue.add(sample);

        if (poppedValue.isPresent()) {
            update(sample, poppedValue.get());
        } else {
            update(sample);
        }
    }

    private void update(double nextValue) {
        double nextMean = mean + (nextValue - mean) / getCount();
        double nextDSquared = dSquared + (nextValue - mean) * (nextValue - nextMean);
        this.mean = nextMean;
        this.dSquared = nextDSquared;
    }

    private void update(double nextValue, double oldValue) {
        double nextMean = mean + (nextValue - oldValue) / getCount();
        double nextDSquared = dSquared + ((nextValue - oldValue) * (nextValue - nextMean + oldValue - mean));
        this.mean = nextMean;
        this.dSquared = nextDSquared;
    }

    public boolean isBufferFull() {
        return circularQueue.isFull();
    }

    public double getMean() {
        return mean;
    }

    public int getCount() {
        return circularQueue.size();
    }

    public double getSampleVariance() {
        return getCount() > 1 ? dSquared / (getCount() - 1) : 0.0;
    }

    public double getSampleStandardDeviation() {
        return Math.sqrt(getSampleVariance());
    }

    public double getSampleZScore(double sample) {
        return (sample - mean) / getSampleStandardDeviation();
    }
}
