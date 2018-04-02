package assignment3.twoprogs;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import scala.collection.Iterable;

public class JavaAssignment3SVM {
	private static final Pattern SPACE1 = Pattern.compile("\t[0-1] ");
	public static void main(String[] args) {

		
		System.setProperty("hadoop.home.dir", "C:/winutils");
		//Initialise spark
		SparkConf sparkConf = new SparkConf()
				.setAppName("WordCount")
				.setMaster("local[4]").set("spark.executor.memory", "1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		//Create a HashingTF instance to map into vectors
		final HashingTF hash = new HashingTF(1000);
	    // Load data into lines
		JavaRDD<String> lines = ctx.textFile("C:\\Users\\Furqan\\Desktop\\yelp_labelled.txt", 1);
		//using flatMap to convert each line into a JavaRDD of strings
		JavaRDD<String> words = lines.flatMap((String s) -> Arrays.asList(SPACE1.split(s)).iterator()); 
		//mapping each word to 1.
		JavaPairRDD<String, Integer> ones = words.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));
		//taking the keys of 'ones' and assigning to JavaRDD of strings
		JavaRDD<String> eachRow= ones.keys();

		//taking a sample of 60% of data as training data -> sixty
		JavaRDD<String> sixty = eachRow.sample(false, 0.6);
		System.out.println(sixty.count());
		//taking a sample of 40% of data as test data -> forty
		JavaRDD<String> forty = eachRow.subtract(sixty);
		System.out.println(forty.count());
		// Create LabeledPoint datasets for positive sentiments.
		JavaRDD<LabeledPoint> positive = sixty.map(review -> 
			new LabeledPoint(0,hash.transform(Arrays.asList(review.split("\t0"))))
		);
		// Create LabeledPoint datasets for negative sentiments.
		JavaRDD<LabeledPoint> negative = sixty.map(review -> 
		new LabeledPoint(1,hash.transform(Arrays.asList(review.split("\t1"))))
	);
		
		
		//// Create LabeledPoint dataset of both the sentiments
		JavaRDD<LabeledPoint> unionRDD = positive.union(negative);	
		

	    // Run training algorithm to build the model.
	    int numIterations = 100;
	    SVMModel model = SVMWithSGD.train(unionRDD.rdd(), numIterations);
		
	    
	    System.out.println(forty.collect().get(1).replace("\t[0-1]",""));
	    //Computing raw scores:
	    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = unionRDD.map(p ->
	    new Tuple2<>(model.predict(p.features()), p.label()));
	    
	    //Sample string from the test dataset
	    String str=forty.collect().get(1);
	    System.out.println(str);
	    // applying the same HashingTF feature transformation 
		Vector posTestExample = hash.transform(Arrays.asList(str.replace("\t","")));
		
		Vector negTestExample = hash.transform(Arrays.asList(str.replace("\t","")));
		
		//using the learned model
		System.out.println("Prediction for spam-positive test example: " + model.predict(posTestExample));
		System.out.println("Prediction for spam-negative test example: " + model.predict(negTestExample));
	    
	    
	    //Question 2 part b
		//// Getting evaluation metrics
	    BinaryClassificationMetrics metrics =
	    		  new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
	    		double auROC = metrics.areaUnderROC();

	    		System.out.println("Area under ROC = " + auROC);

	}
	
}

