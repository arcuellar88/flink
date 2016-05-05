/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import com.google.common.annotations.VisibleForTesting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>
 * When an element arrives it gets assigned a key using a {@link KeySelector} and it get's
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes of it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>
 * Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class PreaggregateWindowOperatorV2<K, IN, ACC, OUT, W extends TimeWindow>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;

	protected final KeySelector<IN, K> keySelector;

	protected final Trigger<? super IN, ? super W> trigger;

	protected final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	protected TypeSerializer<IN> inputSerializer;

	/**
	 * For serializing the key in checkpoints.
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	protected final TypeSerializer<W> windowSerializer;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	protected transient long currentWatermark = Long.MIN_VALUE;

	protected transient Context context = new Context(null, null);

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Processing time timers that are currently in-flight.
	 */
	protected transient Set<Timer<K, W>> processingTimeTimers;
	protected transient PriorityQueue<Timer<K, W>> processingTimeTimersQueue;

	/**
	 * Current waiting watermark callbacks.
	 */
	protected transient Set<Timer<K, W>> watermarkTimers;
	protected transient PriorityQueue<Timer<K, W>> watermarkTimersQueue;
	
	protected transient Map<K, MergingWindowSet<W>> mergingWindowsByKey;

	
	// ------------------------------------------------------------------------
	// Out-of-order & Pre-aggregate
	// ------------------------------------------------------------------------
	private final IN identityValue;
	private final HashMap<K, KeyContext> hmkeyContext;
	private transient ReduceFunction<IN> reduceF;
	
	
	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public PreaggregateWindowOperatorV2(WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		ReduceFunction<IN> reduceF,
		IN identityValue
			) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;
		this.keySelector = requireNonNull(keySelector);
		this.keySerializer = requireNonNull(keySerializer);

		this.windowStateDescriptor = windowStateDescriptor;
		this.trigger = requireNonNull(trigger);

		
		setChainingStrategy(ChainingStrategy.ALWAYS);
		
		//Out-of-orderPre-aggregate 
		this.reduceF=reduceF;
		this.identityValue=identityValue;
		this.hmkeyContext=new LinkedHashMap<K, KeyContext>();
				
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		currentWatermark = -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100);
		}
		if (processingTimeTimers == null) {
			processingTimeTimers = new HashSet<>();
			processingTimeTimersQueue = new PriorityQueue<>(100);
		}

		context = new Context(null, null);

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindowsByKey = new HashMap<>();
		}

		currentWatermark = Long.MIN_VALUE;

	}

	@Override
	public final void close() throws Exception {
		super.close();
		timestampedCollector = null;
		watermarkTimers = null;
		watermarkTimersQueue = null;
		processingTimeTimers = null;
		processingTimeTimersQueue = null;
		context = null;
		mergingWindowsByKey = null;
	}

	@Override
	public void dispose() {
		super.dispose();
		timestampedCollector = null;
		watermarkTimers = null;
		watermarkTimersQueue = null;
		processingTimeTimers = null;
		processingTimeTimersQueue = null;
		context = null;
		mergingWindowsByKey = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		//aggregated2 Record @ 1 : (a,1,1)
		final K key = (K) getStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {
				// If there is a merge, it can only result in a window that contains our new
				// element because we always eagerly merge
				final Tuple1<TriggerResult> mergeTriggerResult = new Tuple1<>(TriggerResult.CONTINUE);


				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {
						context.key = key;
						context.window = mergeResult;

						// store for later use
						mergeTriggerResult.f0 = context.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							context.window = m;
							context.clear();
						}

						// merge the merged state windows into the newly resulting state window
						getStateBackend().mergePartitionedStates(stateWindowResult,
								mergedStateWindows,
								windowSerializer,
								(StateDescriptor<? extends MergingState<?,?>, ?>) windowStateDescriptor);
					}
				});

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				AppendingState<IN, ACC> windowState = getPartitionedState(stateWindow, windowSerializer, windowStateDescriptor);
				windowState.add(element.getValue());

				context.key = key;
				context.window = actualWindow;

				// we might have already fired because of a merge but still call onElement
				// on the (possibly merged) window
				TriggerResult triggerResult = context.onElement(element);

				TriggerResult combinedTriggerResult = TriggerResult.merge(triggerResult, mergeTriggerResult.f0);

				processTriggerResult(combinedTriggerResult, actualWindow);
			}

		} else 
		{
			
			KeyContext<K,W,IN> kc=hmkeyContext.get(key);
			//Check if first element of the key
			if (kc == null) 
			{
				//Create the keykontext for the key
				kc=new KeyContext<K,W,IN>(reduceF, inputSerializer, identityValue, 64);
				hmkeyContext.put(key, kc);
			}
			
			// Unordered data
			if(kc.lastTimestamp>element.getTimestamp())
			{
				
				//Search for the partial that should be updated
				int partial_id=kc.getPartial(element.getTimestamp());
				
				if(partial_id>=0)
				{
					kc.update(partial_id,element.getValue());
				}
				
				
				for (W window: elementWindows) {
					
						AppendingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer,windowStateDescriptor);
						windowState.add(element.getValue());
						
						context.key = key;
						context.window = window;
						TriggerResult triggerResult = context.onElement(element);

						processTriggerResult(triggerResult, window);
						
				}
			}
			else
			{
				// Update partials 
				kc.updatePartials(elementWindows, element.getValue(),element.getTimestamp());
				
				for (W window: elementWindows) 
				{
					AppendingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer,windowStateDescriptor);
					windowState.add(element.getValue());
					context.key = key;
					context.window = window;
					TriggerResult triggerResult = context.onElement(element);
					processTriggerResult(triggerResult, window);
				}
				
				
			}
		}
	}

	/**
	 * Retrieves the {@link MergingWindowSet} for the currently active key. The caller must
	 * ensure that the correct key is set in the state backend.
	 */
	@SuppressWarnings("unchecked")
	protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
		MergingWindowSet<W> mergingWindows = mergingWindowsByKey.get((K) getStateBackend().getCurrentKey());
		if (mergingWindows == null) {
			// try to retrieve from state

			TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[] {windowSerializer, windowSerializer} );
			ListStateDescriptor<Tuple2<W, W>> mergeStateDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
			ListState<Tuple2<W, W>> mergeState = getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE, mergeStateDescriptor);

			mergingWindows = new MergingWindowSet<>((MergingWindowAssigner<? super IN, W>) windowAssigner, mergeState);
			mergeState.clear();

			mergingWindowsByKey.put((K) getStateBackend().getCurrentKey(), mergingWindows);
		}
		return mergingWindows;
	}


	/**
	 * Process {@link TriggerResult} for the currently active key and the given window. The caller
	 * must ensure that the correct key is set in the state backend and the context object.
	 */
	@SuppressWarnings("unchecked")
	protected void processTriggerResult(TriggerResult triggerResult, W window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}

		AppendingState<IN, ACC> windowState;

		MergingWindowSet<W> mergingWindows = null;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(window);
			windowState = getPartitionedState(stateWindow, windowSerializer, windowStateDescriptor);

		} else {
			windowState = getPartitionedState(window, windowSerializer, windowStateDescriptor);
		}

		if (triggerResult.isFire()) {
			timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
			ACC contents = windowState.get();
			
			KeyContext<K, TimeWindow, IN> kContext=hmkeyContext.get(context.key);
			
			//aggregate result
			System.out.println(context.window+" Aggregate: "+kContext.getWindowContent(context.window));
			
			userFunction.apply(context.key, context.window, contents, timestampedCollector);

		}
		if (triggerResult.isPurge()) {
			windowState.clear();
			if (mergingWindows != null) {
				mergingWindows.retireWindow(window);
			}
			context.clear();
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {
		processTriggersFor(mark);

		output.emitWatermark(mark);

		this.currentWatermark = mark.getTimestamp();
	}

	private void processTriggersFor(Watermark mark) throws Exception {
		boolean fire;

		do {
			Timer<K, W> timer = watermarkTimersQueue.peek();
			if (timer != null && timer.timestamp <= mark.getTimestamp()) {
				fire = true;

				watermarkTimers.remove(timer);
				watermarkTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				TriggerResult triggerResult = context.onEventTime(timer.timestamp);
				processTriggerResult(triggerResult, context.window);
			} else {
				fire = false;
			}
		} while (fire);
	}

	@Override
	public final void trigger(long time) throws Exception {
		boolean fire;

		do {
			Timer<K, W> timer = processingTimeTimersQueue.peek();
			if (timer != null && timer.timestamp <= time) {
				fire = true;

				processingTimeTimers.remove(timer);
				processingTimeTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				TriggerResult triggerResult = context.onProcessingTime(timer.timestamp);
				processTriggerResult(triggerResult, context.window);
			} else {
				fire = false;
			}
		} while (fire);

		// Also check any watermark timers. We might have some in here since
		// Context.registerEventTimeTimer sets a trigger if an event-time trigger is registered
		// that is already behind the watermark.
		processTriggersFor(new Watermark(currentWatermark));
	}

protected class KeyContext<K,W extends TimeWindow,T>{
		
	/**
	 * PartialQueueElement ts * 
	 */
	protected class PartialQueueElement
	{
		//protected int partial_id;
		private long ts;
		private ArrayList<W> sWindows;
		private ArrayList<W> eWindows;
		
		//protected long end_ts;
		
		public PartialQueueElement(long ts, List<W> sWindows,List<W> eWindows) {
			this.ts = ts;
			this.sWindows = new ArrayList<W>(sWindows);
			this.eWindows=new ArrayList<W>(eWindows);
		}
		
		public int compareTo( PartialQueueElement e )
		{
			return Long.compare(ts, e.ts);
		}

		public void addStartWindow(W window) {
			sWindows.add(window);			
		}

		public void addEndWindow(W window) {
			eWindows.add(window);
		}

		
	}
		/**
		 * PartialRange [start_ts,end_ts)
		 */
		protected class Partial<T>
		{
			protected int partial_id;
			protected long start_ts;
			protected long end_ts;
			protected T partial;
			
			protected T identityValue;
			
			public Partial(int partial_id, long start_ts, long end_ts, T identityValue) {
				this.partial_id = partial_id;
				this.start_ts = start_ts;
				this.end_ts = end_ts;
				partial=identityValue;
				this.identityValue=identityValue;
			}

			public Partial(int partial_id, T partial, long start_ts, long end_ts, T identityValue) {
				this.partial_id = partial_id;
				this.start_ts = start_ts;
				this.end_ts = end_ts;
				this.partial=partial;
				this.identityValue=identityValue;
			}
			
			public Partial<T> clone()
			{
				return new Partial<T>(partial_id, partial, start_ts, end_ts,identityValue);
			}
			/**
			 * If the argument is between [start_ts,end_ts) then 0 is returned.
			 * If the argument is greater than end_ts then -1 is returned.
			 * If the argument is less than start_ts then 1 is returned.
			 * @param ts
			 * @return
			 */
			public int isPartial(long ts)
			{
				// Range [start_ts,end_ts)
				if(ts>=start_ts&&ts<end_ts)
					return 0;
				else if(ts>=start_ts)
					return -1;
				
				return 1;
			}
		}

		private K key;
		
		private List<Partial<T>>activePartials;
		
		
		/**
		 * Pre-aggregate tree
		 */
		private WindowAggregator<T> aggregator;
		
		private Map<W,Integer> hmWindowBegins;
		
		private Map<W,Integer> hmWindowEnds;
		
		private Integer partial_id;
		
		private T identityValue;
		
		private Long lastTimestamp; 
		
		private Partial<T> currentPartial;
		
		private ReduceFunction<T> reduceF;
		
		private TreeMap<Long,PartialQueueElement> partialsQueue;
		
		public KeyContext(ReduceFunction<T> reduceF, TypeSerializer<T> inputSerializer, T identityValue, int capacity)
		{
			this.reduceF=reduceF;
			this.partial_id=1;
			this.aggregator= new EagerHeapAggregator<T>(reduceF, inputSerializer, identityValue, capacity);
				
			this.hmWindowBegins=new HashMap<W, Integer>();
			this.hmWindowEnds =new HashMap<W, Integer>();
			
			this.lastTimestamp=Long.MIN_VALUE;
			
			this.activePartials=new ArrayList<Partial<T>>();
		
			this.partialsQueue= new TreeMap<Long,PartialQueueElement>();
			
			this.identityValue=identityValue;
			
			this.currentPartial= new Partial<T>(partial_id, Long.MIN_VALUE, Long.MIN_VALUE, identityValue);
			this.activePartials.add(currentPartial);
			
		}
		
		public void update(int partial_id, T value) throws Exception {
			
			if(partial_id==currentPartial.partial_id)
			{
				currentPartial.partial=reduceF.reduce(currentPartial.partial, value);
			}
			else
			{
				aggregator.update(partial_id, value);
			}
			
		}

		public T getWindowContent(W window) throws Exception 
		{
			System.out.println(window+" Start: "+hmWindowBegins.get(window)+" End: "+hmWindowEnds.get(window));
			if(hmWindowBegins.get(window)==null||hmWindowEnds.get(window)==null)
			{
				return identityValue;
			}
			
			return aggregator.aggregate(hmWindowBegins.get(window), hmWindowEnds.get(window));
		}

		
		public void updatePartials(Collection<W> elementWindows,
				T v, long ts) throws Exception {
			
			for (W window : elementWindows) {
				
				if(window.getStart()>lastTimestamp)
				{
					if(!partialsQueue.containsKey(window.getStart()))
					{
						partialsQueue.put(window.getStart(),new PartialQueueElement(window.getStart(), Collections.singletonList(window), Collections.EMPTY_LIST));
					}
					else
					{
						PartialQueueElement pqe=partialsQueue.get(window.getStart());
						pqe.addStartWindow(window);
					}
					if(!partialsQueue.containsKey(window.getEnd()))
					{
						partialsQueue.put(window.getEnd(),new PartialQueueElement(window.getEnd(), Collections.EMPTY_LIST,Collections.singletonList(window)));
					}
					else
					{
						PartialQueueElement pqe=partialsQueue.get(window.getEnd());
						pqe.addEndWindow(window);
					}
				}
				
			}
			
			Long k=null;
			if(partialsQueue.size()>0)
			{
				//continue with next ts
				k=partialsQueue.firstKey();
			}
			
			while(k!=null&&k<ts)
			{
				//Remove head of the queue
				PartialQueueElement pqe=partialsQueue.remove(k);
				
				currentPartial.end_ts=pqe.ts;
				for(W w:pqe.eWindows)
				{
					hmWindowEnds.put(w, currentPartial.partial_id);
				}
				
				
				this.aggregator.add(currentPartial.partial_id, currentPartial.partial);
				partial_id++;
				
				currentPartial= new Partial<T>(partial_id, pqe.ts, Long.MAX_VALUE,identityValue);
				this.activePartials.add(currentPartial);
				
				for(W w:pqe.sWindows)
				{
					hmWindowBegins.put(w, currentPartial.partial_id);
				}
				
				if(partialsQueue.size()>0)
				{
					//continue with next ts
					k=partialsQueue.firstKey();
				}
				else {
					k=null;
				}
					
				}
			
			//Add element to the current partial
			currentPartial.partial=reduceF.reduce(currentPartial.partial, v);
			
			//Update Last time stamp
			lastTimestamp=ts;
		}


//		public void addPartialRange(long partial_end) throws Exception {
//			aggregator.add(partial_id, partial);
//			aPartialRange.add(new Partial<T>(partial_id, currentPartial_ts, partial_end,identityValue));
//		}
		
		public boolean hasWindow(W window){
			return hmkeyContext.containsKey(window);
		}

		
		/**
		 * Binary search for the partial -> partial.start>=ts && partial.end<ts
		 * @param ts Time stamp to search for
		 * @return
		 */
		public int getPartial(long ts) {
	        int lo = 0;
	        int hi = activePartials.size() - 1;
	        while (lo <= hi) {
	            // Key is in a[lo..hi] or not present.
	            int mid = lo + (hi - lo) / 2;
	            if      (activePartials.get(mid).isPartial(ts)>0) hi = mid - 1;
	            else if (activePartials.get(mid).isPartial(ts)<0) lo = mid + 1;
	            else return activePartials.get(mid).partial_id;
	        }
	        return -1;
	    }
		
		//		public boolean isPartial(long ts)
		//		{
		//			// Range [start_ts,end_ts)
		//			if(ts>=start_ts&&ts<end_ts)
		//				return true;
		//			
		//			return false;
		//		}

	}

	
	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	public class Context implements Trigger.OnMergeContext {
		protected K key;
		protected W window;

		protected Collection<W> mergedWindows;

		public Context(K key, W window) {
			this.key = key;
			this.window = window;
		}

		public long getCurrentWatermark() {
			return currentWatermark;
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			requireNonNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			requireNonNull(name, "The name of the state must not be null");
			requireNonNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return PreaggregateWindowOperatorV2.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					PreaggregateWindowOperatorV2.this.getStateBackend().mergePartitionedStates(window,
							mergedWindows,
							windowSerializer,
							stateDescriptor);
				} catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.add(timer)) {
				processingTimeTimersQueue.add(timer);
				getRuntimeContext().registerTimer(time, PreaggregateWindowOperatorV2.this);
			}
		}

		@Override
		public void registerEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.add(timer)) {
				watermarkTimersQueue.add(timer);
			}

			if (time <= currentWatermark) {
				// immediately schedule a trigger, so that we don't wait for the next
				// watermark update to fire the watermark trigger
				getRuntimeContext().registerTimer(System.currentTimeMillis(), PreaggregateWindowOperatorV2.this);
			}
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.remove(timer)) {
				processingTimeTimersQueue.remove(timer);
			}
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.remove(timer)) {
				watermarkTimersQueue.remove(timer);
			}

		}

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public TriggerResult onMerge(Collection<W> mergedWindows) throws Exception {
			this.mergedWindows = mergedWindows;
			return trigger.onMerge(window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
				"key=" + key +
				", window=" + window +
				'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", window=" + window +
				'}';
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	@SuppressWarnings("unchecked")
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {

		if (mergingWindowsByKey != null) {
			TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[] {windowSerializer, windowSerializer} );
			ListStateDescriptor<Tuple2<W, W>> mergeStateDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
			for (Map.Entry<K, MergingWindowSet<W>> key: mergingWindowsByKey.entrySet()) {
				setKeyContext(key.getKey());
				ListState<Tuple2<W, W>> mergeState = getStateBackend().getPartitionedState(null, VoidSerializer.INSTANCE, mergeStateDescriptor);
				mergeState.clear();
				key.getValue().persist(mergeState);
			}
		}

		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out =
			getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		out.writeInt(watermarkTimersQueue.size());
		for (Timer<K, W> timer : watermarkTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		out.writeInt(processingTimeTimers.size());
		for (Timer<K, W> timer : processingTimeTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		taskState.setOperatorState(out.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		super.restoreState(taskState, recoveryTimestamp);

		final ClassLoader userClassloader = getUserCodeClassloader();

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(userClassloader);

		int numWatermarkTimers = in.readInt();
		watermarkTimers = new HashSet<>(numWatermarkTimers);
		watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));
		for (int i = 0; i < numWatermarkTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			watermarkTimers.add(timer);
			watermarkTimersQueue.add(timer);
		}

		int numProcessingTimeTimers = in.readInt();
		processingTimeTimers = new HashSet<>(numProcessingTimeTimers);
		processingTimeTimersQueue = new PriorityQueue<>(Math.max(numProcessingTimeTimers, 1));
		for (int i = 0; i < numProcessingTimeTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			processingTimeTimers.add(timer);
			processingTimeTimersQueue.add(timer);
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}
