package lol.schroeder;

import lombok.*;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LitterBoxEvent {
    String deviceId;
    Instant startTime;
    Instant endTime;
    Double catWeight;
    Double eliminationWeight;
}
