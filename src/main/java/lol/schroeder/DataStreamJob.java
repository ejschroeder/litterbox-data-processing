package lol.schroeder;

import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;


public class DataStreamJob {

	private final SourceFunction<ScaleWeightEvent> source;
	private final SinkFunction<LitterBoxEvent> sink;
	private final SinkFunction<LitterBoxEvent> invalidEventSink;

	public DataStreamJob(
			SourceFunction<ScaleWeightEvent> source,
			SinkFunction<LitterBoxEvent> sink,
			SinkFunction<LitterBoxEvent> invalidEventSink) {
		this.source = source;
		this.sink = sink;
		this.invalidEventSink = invalidEventSink;
	}

	@SneakyThrows
	public JobExecutionResult execute() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<ScaleWeightEvent> weightEvents = env.addSource(source);

		WatermarkStrategy<ScaleWeightEvent> watermarkStrategy =
				WatermarkStrategy.<ScaleWeightEvent>forMonotonousTimestamps()
						.withTimestampAssigner((swe, t) -> swe.getTime() * 1000);

		OutputTag<LitterBoxEvent> invalidEventsOutputTag = new OutputTag<>("invalid-litterbox-events", TypeInformation.of(LitterBoxEvent.class));

		SingleOutputStreamOperator<LitterBoxEvent> litterBoxEvents = weightEvents
				.assignTimestampsAndWatermarks(watermarkStrategy)
				.keyBy(ScaleWeightEvent::getDeviceId)
				.process(new WeightToLitterBoxEventMapper())
				.process(new ValidLitterBoxEventFilter(invalidEventsOutputTag));

		litterBoxEvents.addSink(sink);
		litterBoxEvents.getSideOutput(invalidEventsOutputTag).addSink(invalidEventSink);

		return env.execute("Litter Box Data Processing");
	}

	public static void main(String[] args) {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		new DataStreamJob(
				buildRabbitSource(parameters),
				buildJdbcSink(parameters),
				buildInvalidEventJdbcSink(parameters)
		).execute();
	}

	public static SinkFunction<LitterBoxEvent> buildJdbcSink(ParameterTool parameters) {
		String jdbcUrl = parameters.getRequired("jdbcUrl");
		String jdbcDriver = parameters.get("jdbcDriver", "org.postgresql.Driver");
		String jdbcUsername = parameters.getRequired("jdbcUsername");
		String jdbcPassword = parameters.getRequired("jdbcPassword");

		return JdbcSink.sink(
				"insert into litterbox_events (device_id, start_time, end_time, cat_weight, elimination_weight) values (?, ?, ?, ?, ?)",
				(statement, litterBoxEvent) -> {
					statement.setString(1, litterBoxEvent.getDeviceId());
					statement.setTimestamp(2, Timestamp.from(litterBoxEvent.getStartTime()));
					statement.setTimestamp(3, Timestamp.from(litterBoxEvent.getEndTime()));
					statement.setDouble(4, litterBoxEvent.getCatWeight());
					statement.setDouble(5, litterBoxEvent.getEliminationWeight());
				},
				JdbcExecutionOptions.builder()
						.withBatchSize(1000)
						.withBatchIntervalMs(200)
						.withMaxRetries(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl(jdbcUrl)
						.withDriverName(jdbcDriver)
						.withUsername(jdbcUsername)
						.withPassword(jdbcPassword)
						.build()
		);
	}

	public static SinkFunction<LitterBoxEvent> buildInvalidEventJdbcSink(ParameterTool parameters) {
		String jdbcUrl = parameters.getRequired("jdbcUrl");
		String jdbcDriver = parameters.get("jdbcDriver", "org.postgresql.Driver");
		String jdbcUsername = parameters.getRequired("jdbcUsername");
		String jdbcPassword = parameters.getRequired("jdbcPassword");

		return JdbcSink.sink(
				"insert into invalid_litterbox_events (device_id, start_time, end_time, cat_weight, elimination_weight) values (?, ?, ?, ?, ?)",
				(statement, litterBoxEvent) -> {
					statement.setString(1, litterBoxEvent.getDeviceId());
					statement.setTimestamp(2, Timestamp.from(litterBoxEvent.getStartTime()));
					statement.setTimestamp(3, Timestamp.from(litterBoxEvent.getEndTime()));
					statement.setDouble(4, litterBoxEvent.getCatWeight());
					statement.setDouble(5, litterBoxEvent.getEliminationWeight());
				},
				JdbcExecutionOptions.builder()
						.withBatchSize(1000)
						.withBatchIntervalMs(200)
						.withMaxRetries(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl(jdbcUrl)
						.withDriverName(jdbcDriver)
						.withUsername(jdbcUsername)
						.withPassword(jdbcPassword)
						.build()
		);
	}

	public static RMQSource<ScaleWeightEvent> buildRabbitSource(ParameterTool parameters) {
		String rabbitHost = parameters.getRequired("rabbitHost");
		int rabbitPort = parameters.getInt("rabbitPort", 5672);
		String rabbitUser = parameters.getRequired("rabbitUser");
		String rabbitPassword = parameters.getRequired("rabbitPassword");
		String rabbitVirtualHost = parameters.get("rabbitVirtualHost", "/");
		String queue = parameters.get("queue");

		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost(rabbitHost)
				.setPort(rabbitPort)
				.setUserName(rabbitUser)
				.setPassword(rabbitPassword)
				.setVirtualHost(rabbitVirtualHost)
				.build();

		return new RMQSource<>(
				connectionConfig,
				queue,
				new ScaleWeightEventDeserializationSchema()
		);
	}
}
