package thesis.outoforder.WindowFunctions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

@SuppressWarnings("serial")
public class MaxPreAggregator extends ComparablePreAggregator {
		
	private final static Tuple3<String, Double, Long> fIdentityValue= new Tuple3<String,Double,Long>("",Double.MIN_VALUE,0L);
	
	
	public MaxPreAggregator() {
		super(1,AggregationFunction.AggregationType.MAX,true,new ExecutionConfig(),fIdentityValue);
	}
	
	public MaxPreAggregator(ExecutionConfig config,Tuple3<String, Double, Long> identityValue) {
		super(1,AggregationFunction.AggregationType.MAX,true,config,identityValue);
	}


}
