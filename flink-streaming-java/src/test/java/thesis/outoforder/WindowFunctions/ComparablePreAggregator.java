package thesis.outoforder.WindowFunctions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.runtime.operators.windowing.AggregationStats;

public class ComparablePreAggregator extends PreaggregateReduceFunction<Tuple3<String,Double,Long>> {

	private ComparableAggregator<Tuple3<String,Double,Long>> comparableAggregator;
	private AggregationStats stats = AggregationStats.getInstance();
	
	public ComparablePreAggregator(
			int positionToAggregate,
			AggregationType aggregationType,
			boolean first, ExecutionConfig config,
			Tuple3<String,Double,Long> identityValue) {
		super(identityValue);
		
		TypeInformation<Tuple3<String, Double, Long>> info = TypeInfoParser.parse("Tuple3<String,Double,Long>");;
		

		this.comparableAggregator= new ComparableAggregator<Tuple3<String, Double, Long>>(positionToAggregate, info, aggregationType, first, config);
	}

	@Override
	public Tuple3<String, Double, Long> reduce(
			Tuple3<String, Double, Long> value1,
			Tuple3<String, Double, Long> value2) throws Exception {
		stats.registerReduce();
		return comparableAggregator.reduce(value1, value2);
	}

}
