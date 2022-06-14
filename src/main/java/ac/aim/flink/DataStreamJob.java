/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ac.aim.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DataStreamJob {

	private static final String KAFKA_BOOTSTRAP_SERVER = "your.kafka.server:29092";
	private static final String KAFKA_TOPIC = "test-topic";

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		env.enableCheckpointing(60000L);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
		env.getCheckpointConfig().setCheckpointTimeout(6000000L);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
		env.getCheckpointConfig().enableUnalignedCheckpoints();

		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
			.setTopics(KAFKA_TOPIC)
			.setGroupId("test-group")
			.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
			.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
			.build();

		DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
			.uid("Kafka Source");

		SingleOutputStreamOperator<String> operator = ds
		.keyBy(new KeySelector<String, String>(){
			@Override
			public String getKey(String value) throws Exception {
				return value != null ? value.substring(0, 1) : " ";
			}
		})
		.window(EventTimeSessionWindows.withGap(Time.minutes(1L)))
		.trigger(new Trigger<String, TimeWindow>() {
			private transient ValueStateDescriptor<String> desc4PreviousValue;
			private transient ValueState<String> stPreviousValue;

			@Override
			public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
				// do nothing
			}

			@Override
			public TriggerResult onElement(String element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
				if (desc4PreviousValue == null) {
					desc4PreviousValue = new ValueStateDescriptor<String>("prevValue", String.class);
					stPreviousValue = ctx.getPartitionedState(desc4PreviousValue);
				}

				if (stPreviousValue.value() == null) {
					stPreviousValue.update("");
				}

				return TriggerResult.FIRE_AND_PURGE;
			}

			@Override
			public TriggerResult onEventTime(long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
				return TriggerResult.CONTINUE;
			}

			@Override
			public TriggerResult onProcessingTime(long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
				return TriggerResult.CONTINUE;
			}

			@Override
			public boolean canMerge() {
				return true;
			}

			@Override
			public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
				// do nothing
			}
		})
		.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
			private transient ValueStateDescriptor<String> desc4PreviousValue;
			private transient ValueState<String> stPreviousValue;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				desc4PreviousValue = new ValueStateDescriptor<String>("prevValue", String.class);
			}

			@Override
			public void process(String key, ProcessWindowFunction<String, String, String, TimeWindow>.Context context,
					Iterable<String> elements, Collector<String> out) throws Exception {
				if (stPreviousValue == null) {
					// windowState() in session window is not supported.
					// stPreviousValue = context.windowState().getState(desc4PreviousValue);
					stPreviousValue = context.globalState().getState(desc4PreviousValue);
				}

				for (String element: elements) {
					if (element.equals(stPreviousValue.value())) {
						continue;
					}
					stPreviousValue.update(element);
					out.collect(element);
				}
			}

			@Override
			public void clear(
					ProcessWindowFunction<String, String, String, TimeWindow>.Context context)
					throws Exception {
				super.clear(context);
				if (stPreviousValue != null) {
					stPreviousValue.clear();
				}
			}
		})
		.uid("ProcessingData")
		.name("ProcessingData");

		operator.print().uid("PrintData").name("PrintData");

		env.execute("Checkpoint with a merging window");
	}
}
