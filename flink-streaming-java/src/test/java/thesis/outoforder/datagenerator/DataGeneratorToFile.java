package thesis.outoforder.datagenerator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorToFile {
	
	public static class DataStatsMapper implements MapFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>>
	{
		 long ts=Long.MIN_VALUE;
         long tuples=0L;
         long tuplesOutOfOrder=0L;
         long avgDelay=0L;
         long maxDelay=Long.MIN_VALUE;
         
         public DataStatsMapper()
         {
        	 
         }
		
		@Override
		public Tuple3<String, Double, Long> map(
				Tuple3<String, Double, Long> t) throws Exception {
			
			if(tuples>0&&tuples%1000000==0)
            {
            	System.out.println("Tuples: "+tuples);
            	System.out.println("TuplesOutOfOrder: "+tuplesOutOfOrder);
            	System.out.println("%OutOfOrder: "+(double)((double)tuplesOutOfOrder/(double)tuples));
            	System.out.println("Avg delay: "+(double)((double)avgDelay/(double)tuplesOutOfOrder));
            	System.out.println("Max delay: "+maxDelay);
            	System.out.println(t);
            	System.out.println();
            }
            tuples++;
            
            if(t.f2>=ts)
            {
            	ts=t.f2;
            }
            else
            {
            	
            	avgDelay+=ts-t.f2;
            	if(ts-t.f2>maxDelay)
            		{
            		maxDelay=ts-t.f2;
            		}
            	
            	tuplesOutOfOrder++;
            }
        
			
			return t;
		}
		
	}
	
	
public static void main(String[] args) throws Exception {
		
		String outputfile = "../setup/data2.txt";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		long nrTuples=5000000;
		double outOfOrderLevel=0.5;
		long maxDelay=999;
		long maxTupleDistance=50;
		
		DataStream<Tuple3<String, Double, Long>> dataSource = env.addSource(new DataGenerator(0,nrTuples,maxDelay,outOfOrderLevel,true,maxTupleDistance)).map(new DataStatsMapper());
		dataSource.writeAsCsv(outputfile, WriteMode.OVERWRITE);
		env.execute();
}

}
