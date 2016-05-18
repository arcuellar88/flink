package thesis.outoforder.WindowFunctions;

import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public class SumPreAggregator extends PreaggregateReduceFunction<Tuple3<String,Double,Long>> {

	
	public final static Tuple3<String,Double,Long>fIdentityValue= new Tuple3<String,Double,Long>("",0.0,0L);
	
	public SumPreAggregator(Tuple3<String,Double,Long> identityValue) {
		super(identityValue);
	}
	public SumPreAggregator() {
		super(fIdentityValue);
	}

	@Override
	public Tuple3<String,Double,Long> reduce(Tuple3<String,Double,Long> t1, Tuple3<String,Double,Long> t2) throws Exception {
		return new Tuple3<String,Double,Long>(t2.f0,t1.f1+t2.f1,t2.f2);
	}

}
