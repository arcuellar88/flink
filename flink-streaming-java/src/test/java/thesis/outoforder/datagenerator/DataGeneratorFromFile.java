package thesis.outoforder.datagenerator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;


//String-Key, Double-AGG,Long-time

public class DataGeneratorFromFile implements SourceFunction<Tuple3<String, Double, Long>> {

		private static final long serialVersionUID = 1L;

		private final static String DATASET_PATH="../setup/dataset.properties";
		private final static String DS="DS";
		private final static String OUTOFORDER="OutOfOrder";
		private final static String AVGDELAY="AvgDelay";
		private final static String MAXDELAY="MaxDelay";
		private static final String PATH = "path";

		private final static String SEP="_";



		volatile boolean isRunning = false;
		
		private int sleepMillis;
		
		private long tuples;
		
		private long maxDelayMsecs;
		//private long waterMarkMaxDelaySecs=2000;	
		//private double outOfOrderLevel;
		
		
		
		
		//Read from file
		private transient BufferedReader reader;
		private transient InputStream gzipStream;
		private String dataFilePath;

		
		public DataGeneratorFromFile(int sleepMillis, long tuples, long maxDelay,int dataset ) throws IOException 
		{
			this.sleepMillis = sleepMillis;
			this.tuples=tuples;
			this.maxDelayMsecs=maxDelay;
			
			ParameterTool parameter = ParameterTool.fromPropertiesFile(DATASET_PATH);
			initializeDataSet(parameter,dataset);
			
		}
		
			private void initializeDataSet(ParameterTool parameter, int dataset) {

				//TODO
				parameter.get(DS+SEP+dataset+SEP+OUTOFORDER);
				parameter.get(DS+SEP+dataset+SEP+AVGDELAY);
				parameter.get(DS+SEP+dataset+SEP+MAXDELAY);
				dataFilePath=parameter.get(DS+SEP+dataset+SEP+PATH);
		}

			@Override
			public void run(SourceContext<Tuple3<String, Double, Long>> ctx)
				throws Exception 
			{
				gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
				reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
				
				generateStream(ctx);
				
				this.reader.close();
				this.reader = null;
				this.gzipStream.close();
				this.gzipStream = null;
			}
			
		private void generateStream(SourceContext<Tuple3<String, Double, Long>> ctx) throws Exception 
		{
			isRunning = true;
			//rnd = new Random(5719);
			long lastTS=Long.MIN_VALUE;
			long lastWM=0l;
			
			while (this.isRunning&&this.tuples>0) 
			{
				Tuple3<String, Double, Long>t=nextTuple();
				//System.out.println(t);
				if(t!=null)
				{
					ctx.collectWithTimestamp(t, t.f2);
					tuples--;
				
					//Update last TS
					lastTS=lastTS<t.f2?t.f2:lastTS;
				
					//Watermark
				//	System.out.println("LastTS: "+lastTS);
				
					if(lastTS-lastWM>maxDelayMsecs)
					{
						ctx.emitWatermark(new Watermark(lastTS-maxDelayMsecs-1));
						lastWM=lastTS-maxDelayMsecs-1;
					//	System.out.println("WM: "+lastWM);
					}
				
				}
			Thread.sleep(sleepMillis);
			}
			
		}
			
		
		private Tuple3<String, Double, Long> nextTuple() throws IOException {
				
			if(reader.ready())
			{
				return tupleFromString(reader.readLine());
			}
			
			return null;
		}
			
		private Tuple3<String, Double, Long> tupleFromString(String readLine) {
			String values[]=readLine.split(",");
			String key=values[0];
			Double value=Double.parseDouble(values[1]);
			Long ts=Long.parseLong(values[2]);
			
			return new Tuple3<String, Double, Long>(key,value,ts);
		}

		@Override
		public void cancel() {
		isRunning = false;
		}

}