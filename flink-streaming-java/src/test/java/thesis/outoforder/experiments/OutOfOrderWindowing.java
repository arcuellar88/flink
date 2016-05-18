package thesis.outoforder.experiments;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.PreaggregateReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

public class OutOfOrderWindowing {

	private static class NewYorkCityareaFilter implements FilterFunction<TaxiRide>
	{

		@Override
		public boolean filter(TaxiRide t) throws Exception {
			return GeoUtils.isInNYC(t.startLon,t.startLat)&GeoUtils.isInNYC(t.endLon,t.endLat);
		}
		
	}
	public static void main(String[] args) throws Exception 
	{
		
//		rideId         : Long // a unique id for each ride
//		time           : String // the start or end time of a ride
//		isStart        : Boolean // flag indicating the event type
//		startLon       : Float // the longitude of the ride start location
//		startLat       : Float // the latitude of the ride start location
//		endLon         : Float // the longitude of the ride end location
//		endLat         : Float // the latitude of the ride end location
//		passengerCnt   : Short // number of passengers on the ride
//		travelDistance : Float // actual travel distance (-1 for start events)
		
		final int popThreshold = 20; // threshold for popular places
		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		
		// get an ExecutionEnvironment
		StreamExecutionEnvironment env = 
		StreamExecutionEnvironment.getExecutionEnvironment();
		  //env.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class );
		  
		 //env.registerTypeWithKryoSerializer(LocalDateTime.class, JodaLocalDateTimeSerializer);
		 
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("//Users/alejandrorodriguez/Documents/6_Universidad/IT4BI/Thesis/Datasets/nycTaxiRides.gz", maxEventDelay, servingSpeedFactor));
		
		//Filter
		DataStream<TaxiRide>filteredRides=rides.filter(new NewYorkCityareaFilter());
		DataStream<Tuple2<Long,Boolean>> mapFilterRides=filteredRides.map(new MapFunction<TaxiRide, Tuple2<Long,Boolean>>() {

			@Override
			public Tuple2<Long, Boolean> map(TaxiRide t) throws Exception {
				return new Tuple2<Long,Boolean>(t.rideId,t.isStart);
			}
		});
		// print the filtered stream
		DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularSpots =
				filteredRides.map(new GridCellMatcher())
				// partition by cell id and event type
				.<KeyedStream<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>>keyBy(0, 1)
				// build sliding window
				.timeWindow(Time.minutes(15), Time.minutes(5))
				.apply(new CountInCell(),new RideCounter())
				.filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
					@Override
					public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
						return count.f3 >= popThreshold;
					}
				})
				// map grid cell to coordinates
				.map(new GridToCoordinates());
		
		
		popularSpots.writeAsCsv("/Users/alejandrorodriguez/Documents/Thesis/datasets/nycTaxiRides_popularSpots.csv",WriteMode.OVERWRITE);
		//mapFilterRides.writeAsCsv("/Users/alejandrorodriguez/Documents/Thesis/datasets/nycTaxiRides_filteredRides.csv",WriteMode.OVERWRITE);
		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
		
			
	}
	
	public static class CountInCell extends PreaggregateReduceFunction<Tuple3<Integer,Boolean,Integer>>{
		
		public CountInCell()
		{
			super(new Tuple3<Integer,Boolean,Integer>(0,false,0));
		}
		
		@Override
		public Tuple3<Integer, Boolean,Integer> reduce(Tuple3<Integer, Boolean,Integer> t1,
				Tuple3<Integer, Boolean,Integer> t2) throws Exception {
		
			return new Tuple3<Integer, Boolean,Integer>(t1.f0,t2.f1,t1.f2+t2.f2+1);
		}
		
		@Override
		public Tuple3<Integer,Boolean,Integer> getIdentityValue()
		{
			return new Tuple3<>(0,false,0);
		}
	}
	
	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements WindowFunction<
			Tuple3<Integer, Boolean,Integer>, // input type
			Tuple4<Integer, Long, Boolean, Integer>, // output type
			Tuple, // key type
			TimeWindow> // window type
	{

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple3<Integer, Boolean,Integer>> values,
				Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {

			int cellId = ((Tuple2<Integer, Boolean>)key).f0;
			boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
			long windowTime = window.getEnd();

			int cnt = 0;
			for(Tuple3<Integer, Boolean,Integer> v : values) {
				cnt += v.f2;
			}

			out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
		}
	}
	/**
	 * @source: data artisans
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple3<Integer, Boolean,Integer>> {

		@Override
		public Tuple3<Integer, Boolean,Integer> map(TaxiRide taxiRide) throws Exception {
			if(taxiRide.isStart) {
				// get grid cell id for start location
				int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
				return new Tuple3<>(gridId, true,0);
			} else {
				// get grid cell id for end location
				int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
				return new Tuple3<>(gridId, false,0);
			}
		}
	}
	
	/**
	 * @source: data artisans
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	public static class GridToCoordinates implements
			MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(
				Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

			return new Tuple5<>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2,
					cellCount.f3);
		}
	}
		
}
