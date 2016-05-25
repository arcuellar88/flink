package thesis.outoforder.WindowFunctions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.runtime.operators.windowing.AggregationStats;

@SuppressWarnings("serial")
public class MaxByPreAggregator extends ComparablePreAggregator {
		
	private final static Tuple3<String,Double,Long> fIdentityValue= new Tuple3<String,Double,Long>("",Double.MIN_VALUE,0L);

	
	public MaxByPreAggregator( )
	{
		super(1,AggregationFunction.AggregationType.MIN,true,new ExecutionConfig(),fIdentityValue);
	}
	
	public MaxByPreAggregator(ExecutionConfig config,Tuple3<String,Double,Long> identityValue) {
		super(1,AggregationFunction.AggregationType.MIN,true,config,fIdentityValue);
	}

	public String toString()
	{
		return "MAXBy";
	}


}
