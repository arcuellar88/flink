package org.apache.flink.streaming.api.windowing.assigners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MultiQueryWindowAssigner extends WindowAssigner<Object, TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private List<SlidingEventTimeWindows> slidingWindows;
	private List<TumblingEventTimeWindows> tumblingWindows;
	private List<EventTimeSessionWindows> sessionWindows;
	
	
	
	public MultiQueryWindowAssigner(List<SlidingEventTimeWindows> slidingWindows,List<TumblingEventTimeWindows> tumblingWindows, List<EventTimeSessionWindows> sessionWindows )
	{
		this.slidingWindows=slidingWindows;
		this.tumblingWindows=tumblingWindows;
		this.sessionWindows=sessionWindows;
	}
	
	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		
		List<TimeWindow> windowAssigners= new ArrayList<TimeWindow>();
		
		for (SlidingEventTimeWindows sw : slidingWindows) {
			windowAssigners=(List<TimeWindow>) CollectionUtils.union(windowAssigners, sw.assignWindows(element, timestamp));
		}
		for (TumblingEventTimeWindows tw : tumblingWindows) {
			windowAssigners=(List<TimeWindow>) CollectionUtils.union(windowAssigners, tw.assignWindows(element, timestamp));
		}
		for (EventTimeSessionWindows sw : sessionWindows) {
			windowAssigners=(List<TimeWindow>) CollectionUtils.union(windowAssigners, sw.assignWindows(element, timestamp));
		}
		
		return windowAssigners;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(
			StreamExecutionEnvironment env) {
		return EventTimeTrigger.create();
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(
			ExecutionConfig executionConfig) {
		
		return new TimeWindow.Serializer();
	}

}
