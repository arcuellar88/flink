package org.apache.flink.api.common.functions;

public abstract class PreaggregateReduceFunction<T> implements ReduceFunction<T> {

	protected T identityValue;
	protected String windowOperator;
	
	public PreaggregateReduceFunction(T identityValue)
	{
		this.identityValue=identityValue;
	}
	
	
	private static final long serialVersionUID = 1L;

	public T getIdentityValue()
	{
		return identityValue;
	}
	
	public String getWindowOperator()
	{
		return "default";
	}
	
	public void setWindowOperator( String wo)
	{
		this.windowOperator=wo;
	}
	
}