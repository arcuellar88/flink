package thesis.outoforder.WindowFunctions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;

@SuppressWarnings("serial")
public class MinByPreAggregator extends ComparablePreAggregator {
		
	private final static Tuple3<String,Double,Long> fIdentityValue= new Tuple3<String,Double,Long>("",Double.MAX_VALUE,0L);
	
	public MinByPreAggregator() {
		super(1,AggregationFunction.AggregationType.MINBY,true,new ExecutionConfig(),fIdentityValue);
	}
	
	public MinByPreAggregator(ExecutionConfig config,Tuple3<String,Double,Long> identityValue) {
		super(1,AggregationFunction.AggregationType.MINBY,true,config,identityValue);
	}


}
