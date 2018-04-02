package assignment3.twoprogs;


	import java.io.Serializable;
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.List;

	import org.apache.spark.SparkConf;
	import org.apache.spark.api.java.JavaPairRDD;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;

	import scala.Tuple2;



	public class WeatherStation  {

		private String city;
		static JavaRDD<Measurement> measurements;
		static ArrayList<WeatherStation> stations = new ArrayList<WeatherStation>();

		//constructor of WeatherStation
		public WeatherStation(String city,JavaRDD<Measurement> m) {
			this.city=city;
			this.measurements=m;
		
		}
		
		//another constructor which just takes in the JavaRDDs of objects of Measurement
		public WeatherStation(JavaRDD<Measurement> m) {
			this.measurements=m;
		}
		//Serialized
		static class Measurement implements Serializable{
			
			int time;
			double temperature;
			
			public Measurement(int time, double temperature) {
				this.time=time;
				this.temperature=temperature;
			}

		}
		
		
			//method countTemperatures  
			static long countTemperatures(double t1){
			//Here measurements is a union of all temperatures
			//recorded by the two weather stations and
			//passed on from the main method by unionRDD
				
			//filtering on the basis of criteria (data between: t1-1 and t1+ 1)	
			JavaRDD<Measurement> filteredRDD = measurements.filter(p -> p.temperature > t1 -1 && p.temperature < t1 +1 );
			//mapping the filtered data
			JavaRDD<Object> mapRDD = filteredRDD.map(p -> p.temperature);
			//Making key, value pairs, each temperature key is associated with value 1  
			JavaPairRDD<Object,Integer> pairRDD = mapRDD.mapToPair(f -> new Tuple2<Object,Integer>(f,1));
			//counting the no. of times a particular temperature is recorded using reduceByKey 
			JavaPairRDD<Object,Integer> counts = pairRDD.reduceByKey((Integer i1, Integer i2) -> i1 +i2);
			
			//creating a list of Tuples containing key value pairs
			List<Tuple2<Object, Integer>> output = counts.collect();
			int a = 0;
			//looping the list output 
			//and counting total no. of 
			//temperatures recorded within the given range
			for(Tuple2<?, ?> tuple : output)
			{
				a=a+(int)tuple._2();
			}
			//outputs the list as a string
			System.out.println(output.toString());

			return a;
			
		}

		
		
		public static void main(String[] args) {
			
			System.setProperty("hadoop.home.dir", "C:/winutils");
			//Initialising spark
			JavaSparkContext context = new JavaSparkContext(new SparkConf()
					.setAppName("Weather Station App")
					.setMaster("local[4]")
					.set("spark.executor.memory", "1g"));
			
			//Collection of parallelized Measurement objects in measurementsRDD
			JavaRDD<Measurement> measurementsRDD = context.parallelize(Arrays.asList(
					new Measurement(10,1.2),
					new Measurement(11,2.2),
					new Measurement(12,3.2),
					new Measurement(13,3.9),
					new Measurement(14,5.2),
					new Measurement(15,6.0)
					));
			
			//Collection of parallelized Measurement objects in measurements1RDD
			JavaRDD<Measurement> measurements1RDD = context.parallelize(Arrays.asList(
					new Measurement(10,1.1),
					new Measurement(12,2.1),
					new Measurement(13,3.1),
					new Measurement(14,3.1),
					new Measurement(15,5.1),
					new Measurement(16,6.1)
					));
			//List of WeatherStation objects
			stations.add(new WeatherStation("Galway",measurementsRDD));
			stations.add(new WeatherStation("Dublin",measurements1RDD));
			//unionRDD contains union of both measurementsRDD & measurements1RDD data
			JavaRDD<Measurement> unionRDD = measurementsRDD.union(measurements1RDD);
		
			//Creating an object of WeatherStation and passing union in it
			new WeatherStation(unionRDD);
			//Calling the countTemperatures method
			try{
			System.out.println("Data within range {+/-1}:"+countTemperatures(3));
			}catch(Exception e){
				System.out.println(e);
			}
			
			//closing JavaSparkContext
			context.close();
		}
	}
