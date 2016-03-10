package org.apache.sysml.api.mlcontext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MLContextExample {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("MLContextExample").setMaster("local");
		// conf.set("spark.driver.allowMultipleContexts", "true");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		NewMLContext ml = new NewMLContext(sc);

		// Example 1
		System.out.println("------------- Example #1 - create script based on string ");
		Script ex1Script = ScriptFactory.createDMLScriptFromString("print('example 1');");
		ml.execute(ex1Script);

		// Example 2
		// print("example 2");
		// x = $X;
		// y = $Y
		// z = $Z
		// print("x + y = " + (x + y));
		// print("!z:" + !z);
		System.out.println("------------- Example #2 - read script from file, set inputs");
		Script ex2Script = ScriptFactory.createDMLScriptFromFile("hello.dml");
		ex2Script.setInputs("X", 99, "Y", 1, "Z", true);
		ml.execute(ex2Script);

		// Example 3
		System.out.println("------------- Example #3 - create a PYDML script based on a string");
		Script ex3Script = new Script("print(\"example 3\")\n", ScriptType.PYDML);
		ml.execute(ex3Script);

		// Example 4
		// x = $X;
		// y = $Y;
		// A = read($Ain);
		// print("x + y = " + (x + y));
		// print("SUM:" + sum(A));
		System.out.println("------------- Example #4 - set inputs, including JavaRDD");
		Script scr = ScriptFactory.createDMLScriptFromFile("hello2.dml");
		scr.setInputs("X", 9, "Y", 11, "A", sc.textFile("m.csv"));
		ml.execute(scr);

		// Example 5
		System.out.println("------------- Example #5 - chained putInput() calls, including JavaRDD");
		scr.putInput("X", 3).putInput("Y", 4).putInput("A", sc.textFile("m.csv"));
		ml.execute(scr);

		// Example 6
		System.out.println("------------- Example #6 - input a map of values, including JavaRDD");
		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("X", 5);
				put("Y", "6");
				put("A", sc.textFile("m.csv"));
			}
		};
		scr.setInputs(inputs);
		ml.execute(scr);

		// Example 7
		System.out.println("------------- Example #7 - customizing execution step");
		scr.putInput("X", 30).putInput("Y", 40).putInput("A", sc.textFile("m.csv"));
		ScriptExecutor scriptExecutor = new ScriptExecutor() {
			protected void optionalGlobalDataFlowOptimization() {
				// turn off global data flow optimization check
				return;
			}
		};
		ml.execute(scr, scriptExecutor);

		// Example 8
		System.out.println("------------- Example #8 - input an RDD");
		Script ex8Script = ScriptFactory.createDMLScriptFromFile("hello2.dml");
		RDD<String> rdd = exampleRDD(sc);
		ex8Script.setInputs("X", 9, "Y", 11, "A", rdd);
		ml.execute(ex8Script);

		// Example 9
		System.out.println("------------- Example #9 - input a DataFrame");
		DataFrame dataFrame = exampleDataFrame(sc);
		Script ex9Script = ScriptFactory.createDMLScriptFromFile("example.dml");
		ex9Script.putInput("ex", "inputting a DataFrame").putInput("m", dataFrame);
		ml.execute(ex9Script);

		// Example 10
		// Script genDataScript = ScriptFactory
		// .createDMLScriptFromUrl("https://raw.githubusercontent.com/apache/incubator-systemml/master/scripts/datagen/genLinearRegressionData.dml");
		// System.out.println("GEN DATA SCRIPT:\n" + genDataScript.getScriptString());
		// genDataScript.putInput("numSamples", 1000)
		// .putInput("numFeatures", 50)
		// .putInput("maxFeatureValue", 5)
		// .putInput("maxWeight", 5)
		// .putInput("addNoise", false)
		// .putInput("b", 0).putInput("sparsity", 0.7)
		// .putInput("format", "csv")
		// .putInput("perc", 0.5);
		// ml.execute(script);

	}

	public static JavaRDD<String> exampleJavaRDD(JavaSparkContext jsc) {
		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> javaRdd = jsc.parallelize(list);
		return javaRdd;
	}

	public static RDD<String> exampleRDD(JavaSparkContext jsc) {
		JavaRDD<String> javaRdd = exampleJavaRDD(jsc);
		RDD<String> rdd = javaRdd.rdd();
		return rdd;
	}

	public static DataFrame exampleDataFrame(JavaSparkContext jsc) {
		JavaRDD<String> javaRdd = exampleJavaRDD(jsc);

		class StringToRow implements Function<String, Row> {
			private static final long serialVersionUID = 1L;

			public Row call(String str) throws Exception {
				String[] fields = str.split(",");
				return RowFactory.create((Object[]) fields);
			}
		}
		JavaRDD<Row> rowJavaRdd = javaRdd.map(new StringToRow());
		SQLContext sqlContext = new SQLContext(jsc);
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("C1", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C2", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("C3", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		DataFrame dataFrame = sqlContext.createDataFrame(rowJavaRdd, schema);
		return dataFrame;
	}
}
