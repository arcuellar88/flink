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
		
		private long tuples;
		
		private long startTime;
		
		private long maxDelayMsecs;
		
		private double outOfOrderLevel;
		
		private long waterMarkMaxDelaySecs=2000;	
		
		private boolean watermark;
		
		private long maxTupleDistance;
		
		
		public DataGenerator(int sleepMillis, long tuples, long maxDelay, double outOfOrderLevel,boolean watermark,  long maxTupleDistance) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=maxDelay;
			this.watermark=watermark;
			this.startTime=0L;
			this.outOfOrderLevel=outOfOrderLevel;
			this.maxTupleDistance=200;
		}
		
		public DataGenerator(int sleepMillis, long tuples, long maxDelay, boolean watermark) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=maxDelay;
			this.watermark=watermark;
			this.startTime=0L;
			this.outOfOrderLevel=maxDelayMsecs>0?0.3:0;
			this.maxTupleDistance=200;		}
		
		public DataGenerator(int sleepMillis, long tuples, long maxDelay) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=maxDelay;
			this.watermark=true;
			this.startTime=0L;
			this.outOfOrderLevel=maxDelayMsecs>0?0.3:0;
			this.maxTupleDistance=200;
		}
		
		public DataGenerator(int sleepMillis, long tuples) 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=1000;
			this.waterMarkMaxDelaySecs=2000;
			this.watermark=true;
			this.startTime=0L;
			this.outOfOrderLevel=maxDelayMsecs>0?0.3:0;
			this.maxTupleDistance=50;
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
			
			if(watermark)
				ctx.emitWatermark(new Watermark(t.f2 - 100));
			
			tuples--;
			Thread.sleep(sleepMillis);
			}
		}
			private Tuple3<String, Double, Long> nextTuple() {
				
			long distance=(long)(rnd.nextDouble()*maxTupleDistance);
			
			return new Tuple3<>("Key1",rnd.nextDouble()*1000, this.startTime+=distance);
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
					
					long nextScheduledTuple=(emitSchedule.peek()==null)?-1:emitSchedule.peek().f0;
					long lastTupleTime=t.f2;
					
					//Add elements to the emitSchedule
					while(emitSchedule.isEmpty()//||lastTupleTime<emitSchedule.size()<30)
							||lastTupleTime<nextScheduledTuple+maxDelayMsecs)
					{
						long emitTime=t.f2+getNormalDelayMsecs()*(rnd.nextDouble()<outOfOrderLevel?1:0);
						emitSchedule.add(new Tuple2<Long, Object>(emitTime,t));
						//System.out.println(emitTime);
						//Read next line
						t=nextTuple();
						lastTupleTime=t.f2;
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
			private long getNormalDelayMsecs() {
				long delay = -1;
				long x = maxDelayMsecs/2 ;
				
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