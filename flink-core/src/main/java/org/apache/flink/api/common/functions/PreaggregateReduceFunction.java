package org.apache.flink.api.common.functions;

public abstract class PreaggregateReduceFunction<T> implements ReduceFunction<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public abstract T getIdentityValue();
	
	
}