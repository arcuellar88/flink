package thesis.outoforder.datagenerator;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;


//String-Key, Double-AGG,Long-time

public class DataGenerator implements SourceFunction<Tuple3<String, Double, Long>> {

		private static final long serialVersionUID = 1L;

		volatile boolean isRunning = false;
		private Random rnd;
		private int sleepMillis;
		private int tuples=0;
		private long maxDelayMsecs=1000;
		private long waterMarkMaxDelaySecs=2000;	
		private long startTime=0L;
		
		public DataGenerator(int sleepMillis, int tuples, long maxDelay) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=maxDelay;
		}
		
		public DataGenerator(int sleepMillis, int tuples) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=1000;
			this.waterMarkMaxDelaySecs=2000;
		}

			@Override
			public void run(SourceContext<Tuple3<String, Double, Long>> ctx)
				throws Exception 
			{
				if(maxDelayMsecs>0)
				{
					generateUnorderedStream(ctx);
				}
				else
				{
					generateOrderedStream(ctx);
				}
				
			
			}
			
		private void generateOrderedStream(SourceContext<Tuple3<String, Double, Long>> ctx) throws Exception 
		{
			isRunning = true;
			rnd = new Random(5719);
			while (isRunning&&tuples>0) {
			Tuple3<String, Double, Long>t=nextTuple();
			
			//System.out.println(t);
			ctx.collectWithTimestamp(t, t.f2);
			
			ctx.emitWatermark(new Watermark(t.f2 - 100));
			tuples--;
			Thread.sleep(sleepMillis);
			}
		}
			private Tuple3<String, Double, Long> nextTuple() {
			return new Tuple3<>("Key1",rnd.nextDouble()*100, this.startTime+=(long)rnd.nextInt(15));
		}

			@SuppressWarnings("unchecked")
			private void generateUnorderedStream(SourceContext<Tuple3<String, Double, Long>> ctx) throws Exception 
			{
				PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
						32,
						new Comparator<Tuple2<Long, Object>>() {
							@Override
							public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
								return o1.f0.compareTo(o2.f0);
							}
						});
				
				isRunning = true;
				rnd = new Random();
				
				
						
				//Add first item to the emitSchedule
				Tuple3<String, Double, Long>t=nextTuple();
				Tuple3<String, Double, Long> nextEmitedTuple=t;
				
				long watermarkTime = startTime + waterMarkMaxDelaySecs;
				Watermark nextWatermark = new Watermark(t.f2 - maxDelayMsecs - 1);
				emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
				
				while (isRunning&&tuples>0) 
				{
					
					//long nextScheduledTuple=(emitSchedule.peek()==null)?-1:emitSchedule.peek().f0;
					//long lastTupleTime=t.f2;
					
					//Add elements to the emitSchedule
					while(emitSchedule.isEmpty()||emitSchedule.size()<10)
							//||lastTupleTime<nextScheduledTuple+maxDelayMsecs)
					{
						long emitTime=t.f2+getNormalDelayMsecs();
						emitSchedule.add(new Tuple2<Long, Object>(emitTime,t));
						//System.out.println(emitTime);
						t=nextTuple();
						//lastTupleTime=t.f2;
					}
					
					//Emit first element of the schedule
			
					Tuple2<Long, Object> nextEmitScheduled=emitSchedule.poll();
					
					if(nextEmitScheduled.f1 instanceof Watermark)
					{
						//TODO FIX watermarks
						Watermark nextW=(Watermark)nextEmitScheduled.f1;
						ctx.emitWatermark(nextW);
						//System.out.println("Watermark: "+nextW);

						nextWatermark = new Watermark(nextEmitedTuple.f2 - maxDelayMsecs - 1);
						watermarkTime = nextEmitScheduled.f0 + waterMarkMaxDelaySecs;
						emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
					}
					else
					{
						nextEmitedTuple=(Tuple3<String, Double, Long>)nextEmitScheduled.f1;
						ctx.collectWithTimestamp(nextEmitedTuple, nextEmitedTuple.f2);
						//System.out.println("Tuple: "+nextEmitedTuple);
						tuples--;
						Thread.sleep(sleepMillis);
					}
				
					
					
			}


			
				
		}
			
			/**
			 * Taken from Data artisans
			 * @param rand
			 * @return
			 */
			public long getNormalDelayMsecs() {
				long delay = -1;
				long x = maxDelayMsecs / 2;
				//TODO check delay>maxDelaysecs
				while(delay < 0 || delay > maxDelayMsecs) {
					delay = (long)(rnd.nextGaussian() * x) + x;
				}
				return delay;
			}
			
		@Override
		public void cancel() {
		isRunning = false;
		}

}