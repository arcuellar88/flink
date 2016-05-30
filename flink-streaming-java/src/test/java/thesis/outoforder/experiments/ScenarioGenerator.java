package thesis.outoforder.experiments;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Random;

public class ScenarioGenerator {
	
	private final static  Random rnd= new Random(5791);
	private static int size;
	private static int slide;

	 public static void main(String[] args) {
			
			Properties prop = new Properties();
			OutputStream output = null;

			try {

				output = new FileOutputStream("../setup/experiments1.properties");

				int nr_scenarios=10;
				String nr_tuples="1000000,3000000,5000000";
				
				prop.setProperty("SCENARIOS", ""+nr_scenarios);
				
				for (int i = 1; i <= nr_scenarios; i++) {
					prop.setProperty(Scenario.SCENARIO+Scenario.SEP+i+Scenario.SEP+Scenario.WINDOW_OPERATOR, "V0,V1,V2");
					prop.setProperty(Scenario.SCENARIO+Scenario.SEP+i+Scenario.SEP+Scenario.WINDOW_FUNCTION, "SUM");
					prop.setProperty(Scenario.SCENARIO+Scenario.SEP+i+Scenario.SEP+Scenario.NR_TUPLES, nr_tuples);
					prop.setProperty(Scenario.SCENARIO+Scenario.SEP+i+Scenario.SEP+Scenario.NR_QUERIES, ""+i);
					generateWindows(prop,i);
				}						


				
				// save properties to project root folder
				prop.store(output, null);

			} catch (IOException io) {
				io.printStackTrace();
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}			
		}

	private static void generateWindows(Properties prop, int scenario) {
		String slideS="";
		String sizeS="";
		slide=0;
		size=0;
		for (int i = 1; i <= scenario; i++) {
			String slideSize[]=generateSlidingWindow(prop,scenario,i).split("\t");
			slideS+=slideSize[0]+(i==scenario?"":",");
			sizeS+=slideSize[1]+(i==scenario?"":",");
		}
		
		prop.setProperty(Scenario.SCENARIO+Scenario.SEP+scenario, "NR Queries "+scenario+" SLIDES("+slideS+") WINDOW("+sizeS+")");
		
	}

	private static String generateSlidingWindow(Properties prop, int scenario,
			int i) {
		
		slide=rnd.nextInt(3)+1;
		size=size==0?slide*2:size;
		size=size+rnd.nextInt(5)+1;
		
		//					SCENARIO_1_QUERY_1_TYPE=SLIDE
		prop.setProperty(Scenario.SCENARIO+Scenario.SEP+scenario+Scenario.SEP+Scenario.Q+Scenario.SEP+i+Scenario.SEP+Scenario.TYPE, Scenario.SLIDE);
		//					SCENARIO_1_QUERY_1_WINDOW_SIZE=9
		prop.setProperty(Scenario.SCENARIO+Scenario.SEP+scenario+Scenario.SEP+Scenario.Q+Scenario.SEP+i+Scenario.SEP+Scenario.WINDOW_SIZE, ""+size);
		//					SCENARIO_1_QUERY_1_SLIDE=1
		prop.setProperty(Scenario.SCENARIO+Scenario.SEP+scenario+Scenario.SEP+Scenario.Q+Scenario.SEP+i+Scenario.SEP+Scenario.SLIDE, ""+slide);
		
		return slide+"\t"+size;
		
	}
}
