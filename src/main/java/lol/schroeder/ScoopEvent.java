package lol.schroeder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ScoopEvent {
    private String deviceId;
    private Instant startTime;
    private Instant endTime;
    private double scoopedWeight;
}
