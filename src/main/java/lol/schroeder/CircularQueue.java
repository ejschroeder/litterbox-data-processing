package lol.schroeder;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

public class CircularQueue<T> {
    private final int queueSize;
    private final Queue<T> queue;

    public CircularQueue(int size) {
        this.queueSize = size;
        this.queue = new ArrayDeque<>(size);
    }

    public Optional<T> add(T value) {
        Optional<T> oldValue = Optional.ofNullable(isFull() ? queue.remove() : null);
        queue.add(value);
        return oldValue;
    }

    public boolean isFull() {
        return this.queue.size() == queueSize;
    }

    public int size() {
        return this.queue.size();
    }
}
