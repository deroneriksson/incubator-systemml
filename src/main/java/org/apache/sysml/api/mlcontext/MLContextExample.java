package org.apache.sysml.api.mlcontext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

public class MLContextExample {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("MLContextExample").setMaster("local");
		// conf.set("spark.driver.allowMultipleContexts", "true");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		NewMLContext ml = new NewMLContext(sc);

		// Example 1
		System.out.println("EX1 --------------------------------------------");
		Script ex1Script = ScriptFactory.createDMLScriptFromString("print('example 1');");
		ml.execute(ex1Script);

		// Example 2
		// print("example 2");
		// x = $X;
		// y = $Y
		// z = $Z
		// print("x + y = " + (x + y));
		// print("!z:" + !z);
		System.out.println("EX2 --------------------------------------------");
		Script ex2Script = ScriptFactory.createDMLScriptFromFile("hello.dml");
		ex2Script.setInputs("X", 99, "Y", 1, "Z", true);
		ml.execute(ex2Script);

		// Example 3
		System.out.println("EX3 --------------------------------------------");
		Script ex3Script = new Script("print(\"example 3\")\n", ScriptType.PYDML);
		ml.execute(ex3Script);

		// Example 4
		// x = $X;
		// y = $Y;
		// A = read($Ain);
		// print("x + y = " + (x + y));
		// print("SUM:" + sum(A));
		System.out.println("EX4 --------------------------------------------");
		Script script = ScriptFactory.createDMLScriptFromFile("hello2.dml");
		script.setInputs("X", 9, "Y", 11, "A", sc.textFile("m.csv"));
		ml.execute(script);

		// Example 5
		System.out.println("EX5 --------------------------------------------");
		script.putInput("X", 3).putInput("Y", 4).putInput("A", sc.textFile("m.csv"));
		ml.execute(script);

		// Example 6
		System.out.println("EX6 --------------------------------------------");
		Map<String, Object> inputs = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;
			{
				put("X", 5);
				put("Y", "6");
				put("A", sc.textFile("m.csv"));
			}
		};
		script.setInputs(inputs);
		ml.execute(script);

		// Example 7
		System.out.println("EX7 --------------------------------------------");
		script.putInput("X", 30).putInput("Y", 40).putInput("A", sc.textFile("m.csv"));
		ScriptExecutor scriptExecutor = new ScriptExecutor() {
			protected void optionalGlobalDataFlowOptimization() {
				// turn off global data flow optimization check
				return;
			}
		};
		ml.execute(script, scriptExecutor);

		System.out.println("EX8 --------------------------------------------");
		Script scr = ScriptFactory.createDMLScriptFromFile("hello2.dml");

		List<String> list = new ArrayList<String>();
		list.add("1,2,3");
		list.add("4,5,6");
		list.add("7,8,9");
		JavaRDD<String> nums = sc.parallelize(list);
		RDD<String> rdd = nums.rdd();
		scr.setInputs("X", 9, "Y", 11, "A", rdd);
		ml.execute(scr);

		// Example 8
		// System.out.println("EX9 --------------------------------------------");
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

}
