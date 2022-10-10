package lol.schroeder;

import lombok.*;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScaleWeightEvent {
    private String deviceId;
    private String sensor;
    private Double value;
    private Long time;
}
