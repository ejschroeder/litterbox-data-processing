package lol.schroeder;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Configuration {
    String rabbitHost;
    int rabbitPort;
    String rabbitUser;
    String rabbitPassword;
    String rabbitVirtualHost;
    String rabbitQueue;

    String jdbcUrl;
    String jdbcDriver;
    String jdbcUsername;
    String jdbcPassword;
}
