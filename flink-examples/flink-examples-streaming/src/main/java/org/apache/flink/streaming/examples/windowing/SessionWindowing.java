/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.windowing.outoforder.PreaggregateReduceFunction;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowing {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final boolean fileOutput = params.has("output");

		final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

		input.add(new Tuple3<>("a", 1L, 1));
		input.add(new Tuple3<>("b", 1L, 1));
		input.add(new Tuple3<>("b", 3L, 1));
		input.add(new Tuple3<>("b", 5L, 1));
		input.add(new Tuple3<>("c", 6L, 1));
		// We expect to detect the session "a" earlier than this point (the old
		// functionality can only detect here when the next starts)
		input.add(new Tuple3<>("a", 10L, 1));
		// We expect to detect session "b" and "c" at this point as well
		input.add(new Tuple3<>("c", 11L, 1));

		DataStream<Tuple3<String, Long, Integer>> source = env
				.addSource(new SourceFunction<Tuple3<String,Long,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
						for (Tuple3<String, Long, Integer> value : input) {
							ctx.collectWithTimestamp(value, value.f1);
							ctx.emitWatermark(new Watermark(value.f1 - 1));
							if (!fileOutput) {
								System.out.println("Collected: " + value);
							}
						}
						ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
					}

					@Override
					public void cancel() {
					}
				});

		// We create sessions for each id with max timeout of 3 time units
//		DataStream<Tuple3<String, Long, Integer>> aggregated = source
//				.keyBy(0)
//				.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
//				.sum(2);
		
		DataStream<Tuple3<String, Long, Integer>> aggregated = source
				.keyBy(0)
				.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
				.reduce(new SumReduceF());

		if (fileOutput) {
			aggregated.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			aggregated.print();
		}

		env.execute();
	
	}
	public static class SumReduceF extends PreaggregateReduceFunction<Tuple3<String, Long, Integer>>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, Long, Integer> reduce(
				Tuple3<String, Long, Integer> t1,
				Tuple3<String, Long, Integer> t2) throws Exception {
			
			if(t1.f0.equals("IdentityValue"))
			{
				return new Tuple3<String, Long, Integer>(t2.f0,t2.f1,t1.f2+t2.f2);
			}
			else
			{
				return new Tuple3<String, Long, Integer>(t1.f0,t1.f1,t1.f2+t2.f2);
			}
		}

		@Override
		public Tuple3<String, Long, Integer> getIdentityValue() {
			return new Tuple3<String, Long, Integer>("IdentityValue",0L,0);
		}
		
	}
}
