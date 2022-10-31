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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;


public class DataStreamJob {

	private final SourceFunction<ScaleWeightEvent> scaleEventSource;
	private final SinkFunction<LitterBoxEvent> litterBoxEventSink;
	private final SinkFunction<LitterBoxEvent> invalidEventSink;
	private final SinkFunction<ScoopEvent> scoopEventSink;

	public DataStreamJob(
			SourceFunction<ScaleWeightEvent> scaleEventSource,
			SinkFunction<LitterBoxEvent> litterBoxEventSink,
			SinkFunction<LitterBoxEvent> invalidEventSink,
			SinkFunction<ScoopEvent> scoopEventSink) {
		this.scaleEventSource = scaleEventSource;
		this.litterBoxEventSink = litterBoxEventSink;
		this.invalidEventSink = invalidEventSink;
		this.scoopEventSink = scoopEventSink;
	}

	@SneakyThrows
	public JobExecutionResult execute() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<ScaleWeightEvent> weightEvents = env.addSource(scaleEventSource);

		WatermarkStrategy<ScaleWeightEvent> watermarkStrategy =
				WatermarkStrategy.<ScaleWeightEvent>forMonotonousTimestamps()
						.withTimestampAssigner((swe, t) -> swe.getTime() * 1000);

		OutputTag<LitterBoxEvent> invalidEventsOutputTag = new OutputTag<>("invalid-litterbox-events", TypeInformation.of(LitterBoxEvent.class));

		KeyedStream<ScaleWeightEvent, String> scaleWeightEvents = weightEvents
				.assignTimestampsAndWatermarks(watermarkStrategy)
				.keyBy(ScaleWeightEvent::getDeviceId);

		SingleOutputStreamOperator<LitterBoxEvent> litterBoxEvents = scaleWeightEvents
				.process(new WeightToLitterBoxEventMapper())
				.process(new ValidLitterBoxEventFilter(invalidEventsOutputTag));

		SingleOutputStreamOperator<ScoopEvent> scoopEvents = scaleWeightEvents
				.process(new WeightToScoopEventMapper());

		litterBoxEvents.addSink(litterBoxEventSink);
		litterBoxEvents.getSideOutput(invalidEventsOutputTag).addSink(invalidEventSink);
		scoopEvents.addSink(scoopEventSink);

		return env.execute("Litter Box Data Processing");
	}

	public static void main(String[] args) {
		Configuration config = buildConfiguration(ParameterTool.fromArgs(args));

		new DataStreamJob(
				buildRabbitSource(config),
				buildJdbcSink(config),
				buildInvalidEventJdbcSink(config),
				buildScoopEventJdbcSink(config)
		).execute();
	}

	private static Configuration buildConfiguration(ParameterTool parameters) {
		return Configuration.builder()
				.rabbitHost(parameters.getRequired("rabbitHost"))
				.rabbitPort(parameters.getInt("rabbitPort", 5672))
				.rabbitUser(parameters.getRequired("rabbitUser"))
				.rabbitPassword(parameters.getRequired("rabbitPassword"))
				.rabbitVirtualHost(parameters.get("rabbitVirtualHost", "/"))
				.rabbitQueue(parameters.get("queue"))
				.jdbcUrl(parameters.getRequired("jdbcUrl"))
				.jdbcDriver(parameters.get("jdbcDriver", "org.postgresql.Driver"))
				.jdbcUsername(parameters.getRequired("jdbcUsername"))
				.jdbcPassword(parameters.getRequired("jdbcPassword"))
				.build();
	}

	public static SinkFunction<LitterBoxEvent> buildJdbcSink(Configuration config) {
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
						.withUrl(config.getJdbcUrl())
						.withDriverName(config.getJdbcDriver())
						.withUsername(config.getJdbcUsername())
						.withPassword(config.getJdbcPassword())
						.build()
		);
	}

	public static SinkFunction<LitterBoxEvent> buildInvalidEventJdbcSink(Configuration config) {
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
						.withUrl(config.getJdbcUrl())
						.withDriverName(config.getJdbcDriver())
						.withUsername(config.getJdbcUsername())
						.withPassword(config.getJdbcPassword())
						.build()
		);
	}

	public static SinkFunction<ScoopEvent> buildScoopEventJdbcSink(Configuration config) {
		return JdbcSink.sink(
				"insert into scoop_events (device_id, start_time, end_time, scooped_weight) values (?, ?, ?, ?)",
				(statement, scoopEvent) -> {
					statement.setString(1, scoopEvent.getDeviceId());
					statement.setTimestamp(2, Timestamp.from(scoopEvent.getStartTime()));
					statement.setTimestamp(3, Timestamp.from(scoopEvent.getEndTime()));
					statement.setDouble(4, scoopEvent.getScoopedWeight());
				},
				JdbcExecutionOptions.builder()
						.withBatchSize(1000)
						.withBatchIntervalMs(200)
						.withMaxRetries(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl(config.getJdbcUrl())
						.withDriverName(config.getJdbcDriver())
						.withUsername(config.getJdbcUsername())
						.withPassword(config.getJdbcPassword())
						.build()
		);
	}

	public static RMQSource<ScaleWeightEvent> buildRabbitSource(Configuration config) {
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost(config.getRabbitHost())
				.setPort(config.getRabbitPort())
				.setUserName(config.getRabbitUser())
				.setPassword(config.getRabbitPassword())
				.setVirtualHost(config.getRabbitVirtualHost())
				.build();

		return new RMQSource<>(
				connectionConfig,
				config.getRabbitQueue(),
				new ScaleWeightEventDeserializationSchema()
		);
	}
}
