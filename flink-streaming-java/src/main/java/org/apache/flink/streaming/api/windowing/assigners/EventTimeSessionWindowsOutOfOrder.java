/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into sessions based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * <p>
 * For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(EventTimeSessionWindows.withGap(Time.minutes(1)));
 * } </pre>
 */
public class EventTimeSessionWindowsOutOfOrder extends MergingWindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	protected long sessionTimeout;
	
	protected Collection<Long>sessionTimeouts;
	
	private long maxTimestamp=0L;
	

	protected EventTimeSessionWindowsOutOfOrder(long sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
		this.sessionTimeouts= new ArrayList<>();
		this.sessionTimeouts.add(sessionTimeout);
	}

	public EventTimeSessionWindowsOutOfOrder(Time[] sizes) {
		this.sessionTimeouts= new ArrayList<>();
		for (Time t : sizes) {
			sessionTimeouts.add(t.toMilliseconds());
		}
	}

	public EventTimeSessionWindowsOutOfOrder() {
		this.sessionTimeouts= new ArrayList<>();
	}
	
	public void addGap(Long t)
	{
		sessionTimeouts.add(t);
	}

	public Tuple2<Collection<TimeWindow>,Collection<TimeWindow>> assignWindowsOutOfOrder(Object element, long timestamp) {
		
		Collection<TimeWindow> windows=Collections.singleton(new TimeWindow(timestamp, timestamp + sessionTimeout));
		
		Collection<TimeWindow> windowsOoO=new ArrayList<TimeWindow>();
		//In order
		if(timestamp>maxTimestamp)
		{
			long t=timestamp;
			while(t>maxTimestamp)
			{
				t-=sessionTimeout;
				windowsOoO.add(new TimeWindow(t, t + sessionTimeout));
			}
			maxTimestamp=t;
		}
		//Out-of-order
		else
		{
			//do nothing
		}
		
		return new Tuple2<>(windows,windowsOoO);
	}
	
	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		
		return Collections.singleton(new TimeWindow(timestamp, timestamp + sessionTimeout));
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return EventTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "EventTimeSessionWindows(" + sessionTimeout + ")";
	}

	/**
	 * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns
	 * elements to sessions based on the element timestamp.
	 *
	 * @param size The session timeout, i.e. the time gap between sessions
	 * @return The policy.
	 */
	public static EventTimeSessionWindowsOutOfOrder withGap(Time size) {
		return new EventTimeSessionWindowsOutOfOrder(size.toMilliseconds());
	}
	
	/**
	 * Creates a new {@code SessionWindows} {@link WindowAssigner} that assigns
	 * elements to sessions based on the element timestamp.
	 *
	 * @param size The session timeout, i.e. the time gap between sessions
	 * @return The policy.
	 */
	public static EventTimeSessionWindowsOutOfOrder withGaps(Time ... sizes) {
		return new EventTimeSessionWindowsOutOfOrder(sizes);
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	/**
	 * Merge overlapping {@link TimeWindow}s.
	 */
	public void mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {
		TimeWindow.mergeWindows(windows, c);
	}

	public Collection<TimeWindow> assignWindowsOutOfOrder(Object element,
			long timestamp, long lastStart) {
		// TODO Auto-generated method stub
		return null;
	}

	public Tuple2<Collection<TimeWindow>, Collection<TimeWindow>> assignSessionWindows(
			Object element, long timestamp, long lastStart) {
Collection<TimeWindow> windows=Collections.singleton(new TimeWindow(timestamp, timestamp + sessionTimeout));
		
		Collection<TimeWindow> windowsOoO=new ArrayList<TimeWindow>();
		//In order
		if(timestamp>maxTimestamp)
		{
			long t=timestamp;
			while(t>maxTimestamp)
			{
				t-=sessionTimeout;
				windowsOoO.add(new TimeWindow(t, t + sessionTimeout));
			}
			maxTimestamp=timestamp;
		}
		//Out-of-order
		else
		{
			//do nothing
		}
		
		return new Tuple2<>(windows,windowsOoO);
	}
	
	public Collection<Tuple3<Long,Collection<TimeWindow>, Collection<TimeWindow>>> assignMultiSessionWindows(
			Object element, long timestamp, long lastStart) {
		
		Collection<Tuple3<Long,Collection<TimeWindow>, Collection<TimeWindow>>> c= new ArrayList<>();
		
		for(Long st: sessionTimeouts)
		{
			Collection<TimeWindow> windows=Collections.singleton(new TimeWindow(timestamp, timestamp + st));
			
			Collection<TimeWindow> windowsOoO=new ArrayList<TimeWindow>();
			//In order
			if(timestamp>maxTimestamp)
			{
				long t=timestamp;
				while(t>maxTimestamp)
				{
					t-=st;
					if(t+st<=maxTimestamp||maxTimestamp==0L)
						{
						windowsOoO.add(new TimeWindow(t, t + st));
						}
				}
				
			}
			//Out-of-order
			else
			{
				//do nothing
			}
			 c.add(new Tuple3<>(st,windows,windowsOoO));
		}
		
		maxTimestamp=timestamp;
		return c;
		
	}
}
