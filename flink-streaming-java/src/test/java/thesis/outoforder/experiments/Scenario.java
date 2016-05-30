package thesis.outoforder.experiments;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.windowing.assigners.MultiQueryWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindowsOutOfOrder;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import thesis.outoforder.WindowFunctions.MaxByPreAggregator;
import thesis.outoforder.WindowFunctions.MaxPreAggregator;
import thesis.outoforder.WindowFunctions.MinByPreAggregator;
import thesis.outoforder.WindowFunctions.MinPreAggregator;
import thesis.outoforder.WindowFunctions.SumPreAggregator;

public class Scenario 
{
	public final static String WINDOW_OPERATOR="WINDOW_OPERATOR";
	public final static String Q="QUERY";
	public final static String TYPE="TYPE";
	public final static String WINDOW_SIZE="WINDOW_SIZE";
	public final static String SLIDE="SLIDE";
	public final static String TUMBLING= "TUMBLING";
	public final static String WINDOW_FUNCTION="WINDOW_FUNCTION";
	public final static String SCENARIO="SCENARIO";
	public final static String NR_QUERIES="NR_QUERIES";
	public static final String SESSION = "SESSION";
	public static final String SEP = "_";
	public static final String NR_TUPLES="NR_TUPLES";
	
	
	private String name;
	private int id;
	private String[] windowOperator;
	private WindowAssigner<Object, TimeWindow> windowAssigner;
	private int numberOfQueries;
	private List<PreaggregateReduceFunction<Tuple3<String, Double, Long>>> functions;
	private long[] nrTuples;
	
	
	
	public Scenario(int id, ParameterTool parameter)
	{
		this.id=id;
		initialize(parameter);
	}

	private void initialize(ParameterTool parameter) 
	{
		windowOperator=parameter.get(SCENARIO+SEP+id+SEP+WINDOW_OPERATOR).split(",");
		numberOfQueries=parameter.getInt(SCENARIO+SEP+id+SEP+NR_QUERIES);
		name=parameter.get(SCENARIO+SEP+id);
		
		if(numberOfQueries>1)
			{
				windowAssigner=new MultiQueryWindowAssigner();
				MultiQueryWindowAssigner wa=(MultiQueryWindowAssigner)windowAssigner;
				for (int i = 1; i <= numberOfQueries; i++) 
				{
					String type=parameter.get(SCENARIO+SEP+id+SEP+Q+SEP+i+SEP+TYPE);
					
					switch(type)
					{
						case SLIDE: wa.addSlidingWindowAssigner(loadSlidingWindowAssigner(i,parameter));
						case SESSION: wa.addSessionWindowAssigner(loadSessionWindowAssigner(i,parameter));
					}	
				}
			}
		else
		{
			String type=parameter.get(SCENARIO+SEP+id+SEP+Q+SEP+1+SEP+TYPE);
			windowAssigner=loadWindowAssigner(type,1,parameter);
		}
		
		String tuples[]=parameter.get(SCENARIO+SEP+id+SEP+NR_TUPLES).split(",");
		
		nrTuples= new long[tuples.length];
		
				for (int i = 0; i < tuples.length; i++) {
					nrTuples[i]=Long.parseLong(tuples[i]);
				}
		loadWindowFunctions(parameter);
		
	}

	private void loadWindowFunctions(ParameterTool parameter) {
		String[] list=parameter.get(SCENARIO+SEP+id+SEP+WINDOW_FUNCTION).split(",");
		functions= new ArrayList<PreaggregateReduceFunction<Tuple3<String, Double, Long>>>(list.length);
		
		for (String f: list) 
		{
			PreaggregateReduceFunction<Tuple3<String, Double, Long>> function=null;
			switch(f)
			{
				case "MAX":	
					function=new MaxPreAggregator();
					break;
				case "MIN": 
					function=new MinPreAggregator();
					break;
				case "SUM": 
					function=new SumPreAggregator();
					break;
				case "MINBY": 
					function=new MinByPreAggregator();
					break;
				case "MAXBY":
					function=new MaxByPreAggregator();
					break;
			}
			//function.setWindowOperator(windowOperator);
			functions.add(function);
			
		}
	}

	private WindowAssigner<Object, TimeWindow> loadWindowAssigner(String type, int queryNr,ParameterTool parameter) 
	{
		WindowAssigner<Object, TimeWindow> wa=null;
		switch(type)
		{
			case SLIDE: 
				wa= loadSlidingWindowAssigner(queryNr,parameter);
				break;
			case SESSION: 
				wa= loadSessionWindowAssigner(queryNr,parameter);
				break;
			case TUMBLING: 
				wa=loadTumblingWindowAssigner(queryNr,parameter);
				break;
		}	
		
		return wa;
		
		
	}

	private WindowAssigner<Object, TimeWindow> loadTumblingWindowAssigner(
			int queryNr, ParameterTool parameter) 
			{
		int W_SIZE=parameter.getInt(SCENARIO+SEP+id+SEP+Q+SEP+queryNr+SEP+WINDOW_SIZE);
		return TumblingEventTimeWindows.of(Time.of(W_SIZE, TimeUnit.SECONDS));

	}

	private WindowAssigner<Object, TimeWindow> loadSessionWindowAssigner(int queryNr,
			ParameterTool parameter) {
		//TODO
		return null;

	}
	

	private SlidingEventTimeWindowsOutOfOrder loadSlidingWindowAssigner (int queryNr,
			ParameterTool parameter) 
	{	
		int W_SLIDE=parameter.getInt(SCENARIO+SEP+id+SEP+Q+SEP+queryNr+SEP+SLIDE);
		int W_SIZE=parameter.getInt(SCENARIO+SEP+id+SEP+Q+SEP+queryNr+SEP+WINDOW_SIZE);
		
		return SlidingEventTimeWindowsOutOfOrder.of(Time.of(W_SIZE, TimeUnit.SECONDS), Time.of(W_SLIDE, TimeUnit.SECONDS));

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String[] getWindowOperator() {
		return windowOperator;
	}

	public WindowAssigner<Object, TimeWindow> getWindowAssigner() {
		return windowAssigner;
	}

	public int getNumberOfQueries() {
		return numberOfQueries;
	}

	public List<PreaggregateReduceFunction<Tuple3<String, Double, Long>>> getFunctions() {
		return functions;
	}
	public long[] getNrTuples() {
		return nrTuples;
	}

	

}
