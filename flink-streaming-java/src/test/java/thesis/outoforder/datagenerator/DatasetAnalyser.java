package thesis.outoforder.datagenerator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.flink.api.java.tuple.Tuple3;

public class DatasetAnalyser {

	public static void main(String[] args) {
		
		//
		 try {
			 String inputfile = "../setup/data.txt";
			 
			 // FileReader reads text files in the default encoding.
	            FileReader fileReader = 
	                new FileReader(inputfile);

	            // Always wrap FileReader in BufferedReader.
	            BufferedReader bufferedReader = new BufferedReader(fileReader);

	            String line="";
	            long ts=Long.MIN_VALUE;
	            long tuples=0L;
	            long tuplesOutOfOrder=0L;
	            long avgDelay=0L;
	            long maxDelay=Long.MIN_VALUE;
	            
	            
	            while((line = bufferedReader.readLine()) != null) {
	                String data[]=line.split(",");
	                
	                if(tuples>0&&tuples%1000000==0)
	                {
	                	System.out.println("Tuples: "+tuples);
	                	System.out.println("TuplesOutOfOrder: "+tuplesOutOfOrder);
	                	System.out.println("%OutOfOrder: "+(double)((double)tuplesOutOfOrder/(double)tuples));
	                	System.out.println("Avg delay: "+(double)((double)avgDelay/(double)tuplesOutOfOrder));
	                	System.out.println("Max delay: "+maxDelay);
	                	System.out.println();
	                }
	                tuples++;
	                
	                Tuple3<String, Double, Long> t=Tuple3.of(data[0], Double.parseDouble(data[1]), Long.parseLong(data[2]));
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
	            }   

	            System.out.println("Tuples: "+tuples);
            	System.out.println("TuplesOutOfOrder: "+tuplesOutOfOrder);
            	System.out.println("%OutOfOrder: "+(double)((double)tuplesOutOfOrder/(double)tuples));
            	System.out.println("Avg delay: "+(double)((double)avgDelay/(double)tuplesOutOfOrder));
            	System.out.println("Max delay: "+maxDelay);
	            // Always close files.
	            bufferedReader.close();         
	        }
	        catch(FileNotFoundException ex) {
	            System.out.println(
	                "Unable to open file '" + 
	                 "'");                
	        }
	        catch(IOException ex) {
	            System.out.println(
	                "Error reading file '" 
	                + "'");                  
	            // Or we could just do this: 
	            // ex.printStackTrace();
	        }
	
		

	}

}
