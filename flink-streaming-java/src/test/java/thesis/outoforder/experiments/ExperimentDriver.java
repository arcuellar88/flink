package thesis.outoforder.experiments;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.windowing.AggregationStats;

import thesis.outoforder.datagenerator.DataGenerator;

public class ExperimentDriver {

	private final static String SCENARIOS="SCENARIOS";
	private List<Scenario> scenarios;
	private String RESULT_PATH="../setup/results.txt";
	
	
	public ExperimentDriver(ParameterTool parameter)
	{
		setupScenarios(parameter);
	}
	private void setupScenarios(ParameterTool parameter) {
		
		int nrScenarios=parameter.getInt(SCENARIOS);
		scenarios= new ArrayList<Scenario>(nrScenarios);
		for (int i = 1; i <= nrScenarios; i++)
		{
			scenarios.add(new Scenario(i,parameter));
		}
		
	}
	public void execute() throws Exception {
	
		AggregationStats stats = AggregationStats.getInstance();
	
		//Writer for the results
		FileOutputStream fos= new FileOutputStream(RESULT_PATH,true);
		OutputStreamWriter osw= new OutputStreamWriter(fos,StandardCharsets.UTF_8);
		
		PrintWriter resultWriter = new PrintWriter(osw, true);
		resultWriter.println("SCEN\tTIME\tAGG\tRED\tUPD\tMAXB\tAVGB\tUPD_AVG\tUPD_OUT_OF_ORDER_AVG\tMERGE_AVG\tWINDOW_CNT\tPARTIAL_CNT" +
				"\tTOTAL_OP_TIME\tTOTAL_CPU_TIME\tAVG_OP_TIME\tAVG_CPU_TIME\tWO\tW_FUNCTION\tNR_TUPLES\tAVG_TUPLES_WINDOW\tOUT_OF_ORDER\tDELEY_AVG\tNR_QUERIES");
	
		//run simple program to warm up (The first start up takes more time...)
		runWarmUpTask();
	
		//Variables needed in the loop
		runExperiments(stats, resultWriter);
	
		//close writer
		resultWriter.flush();
		resultWriter.close();
	}
	/**
	 * Runs a small warm up job. This is required because the first job needs longer to start up.
	 *
	 * @throws Exception Any exception which might happens during the execution
	 */
	public void runWarmUpTask() throws Exception {
	
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		DataStream<Tuple3<String, Double, Long>> source = env.addSource(new DataGenerator(10,2));
		source.map((MapFunction<Tuple3<String, Double, Long>, Long>) new MapFunction<Tuple3<String, Double, Long>, Long>() {
			@Override
			public Long map(Tuple3<String, Double, Long> value) throws Exception {
				return value.f2;
			}
		}).print();
		env.execute();
	}
	private void runExperiments(AggregationStats stats, PrintWriter resultWriter) {
		
		for (Scenario s : scenarios) 
		{
			try 
			{
				runAggregation(resultWriter,stats,s);
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
				continue;
			}
		}
	}
	
	private void runAggregation(PrintWriter resultWriter,AggregationStats stats,Scenario s) throws Exception {
	
	JobExecutionResult result= null;
	StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	DataStream<Tuple3<String, Double, Long>> dataSource = env.addSource(new DataGenerator(0,s.getNrTuples(),999,0.15,false,50));
	
	for (String wo : s.getWindowOperator()) 
	{
		for (PreaggregateReduceFunction<Tuple3<String, Double, Long>> f : s.getFunctions()) 
		{
			f.setWindowOperator(wo);
			dataSource
			.keyBy(0)
			.window(s.getWindowAssigner())
			.reduce(f);
			result=env.execute("Scenario: "+s.getName());
			recordExperiment(stats, resultWriter, result, s,wo,f.toString());
		}
	}
		

	}
	
	public void recordExperiment(AggregationStats stats, PrintWriter resultWriter, JobExecutionResult result, Scenario s, String wo, String function) {
		resultWriter.println(s.getId() +" "+s.getName()+"\t"+ result.getNetRuntime() + "\t" + stats.getAggregateCount()
				+ "\t" + stats.getReduceCount() + "\t" + stats.getUpdateCount() + "\t" + stats.getMaxBufferSize() + "\t" + stats.getAverageBufferSize()
				+ "\t" + stats.getAverageUpdTime() + "\t" +stats.getAverageUpdTimeOutOfOrder()+ "\t"+ stats.getAverageMergeTime()
				+ "\t" + (stats.getTotalMergeCount()-1) + "\t" + stats.getPartialCount() + "\t" + stats.getSumOperatorTime()
				+ "\t" + stats.getSumOperatorCPUTime()+ "\t" + stats.getAvgOperatorTime()+ "\t" + stats.getAvgOperatorCPUTime()+"\t"
				+ wo+"\t"+function+"\t"+s.getNrTuples()+"\t"
				+tuplesPerWindowFormat(s.getNrTuples(),stats.getTotalMergeCount())
				+"\t"+oufOfOrderFormat(s.getNrTuples(),stats.getOutOfOrder())
				+"\t"+stats.getAverageDelay()+"\t"+s.getNumberOfQueries());
	
		stats.reset();
		resultWriter.flush();
	}
	
	
	private String oufOfOrderFormat(long nrTuples, int outOfOrder) {
		double avg=(double)outOfOrder/(double)nrTuples*100;
		NumberFormat formatter = new DecimalFormat("#0.00");   
		String answer=formatter.format(avg)+"%";
		return answer;
	}
	private String tuplesPerWindowFormat(long nrTuples, long totalMergeCount) {
		double avg=(double)nrTuples/(double)totalMergeCount;
		NumberFormat formatter = new DecimalFormat("#0.00");     
		
		return formatter.format(avg);
	}
	public static void main(String[] args) {
		
		String propertiesFile = "../setup/experiments.properties";
		try 
		{
			ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
			ExperimentDriver ed=new ExperimentDriver(parameter);
			ed.execute();
			
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
