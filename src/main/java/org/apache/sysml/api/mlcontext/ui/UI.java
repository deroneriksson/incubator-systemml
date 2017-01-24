package org.apache.sysml.api.mlcontext.ui;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.LinearDataGenerator;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.sysml.api.mlcontext.MLContext;
import org.apache.sysml.api.mlcontext.MLContextException;
import org.apache.sysml.api.mlcontext.MatrixMetadata;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptFactory;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class UI {

	protected int port = 1234;
	protected int newPort = 1234;
	protected Server server = null;
	protected MLContext mlContext = null;
	protected Status status = Status.OFF;
	protected Script script = null;

	protected enum Status {
		ON, OFF
	};

	public static void main(String[] args) throws InterruptedException, IOException {
		UI ui = new UI();

		SparkConf conf = new SparkConf().setAppName("UI").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ui.mlContext = new MLContext(sc);

		Script helloScript = ScriptFactory.dml("a=1;\nprint('hello world');\nprint(a+2);\n").setName("howdy");
		ui.register(helloScript);
		ui.mlContext.execute(helloScript);

		// Script uni =
		// ScriptFactory.dmlFromFile("scripts/algorithms/Univar-Stats.dml");
		// uni.setName("Univariate Statistics");
		// String habermanUrl =
		// "http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data";
		// uni.in("A", new URL(habermanUrl));
		// List<String> list = new ArrayList<String>();
		// list.add("1.0,1.0,1.0,2.0");
		// JavaRDD<String> typesRDD = sc.parallelize(list);
		// uni.in("K", typesRDD);
		// uni.in("$CONSOLE_OUTPUT", true);
		// ui.register(uni);

		// ui.mlContext.execute(uni);

		// int numRows = 10;
		// int numCols = 10;
		// RDD<LabeledPoint> rdd =
		// LinearDataGenerator.generateLinearRDD(sc.sc(), numRows, numCols, 1,
		// 2, 0);
		// JavaRDD<LabeledPoint> javaRDD = rdd.toJavaRDD();
		// SQLContext sqlContext = new SQLContext(sc);
		// Dataset<Row> data = sqlContext.createDataFrame(javaRDD,
		// LabeledPoint.class);
		// // note that the labels and features won't match up unless an index
		// // column is added for preprocessing and then selected
		// Dataset<Row> labels = data.select("label");
		// Dataset<Row> features = data.select("features");
		// MatrixMetadata featuresMM = new MatrixMetadata(numRows, numCols);
		// MatrixMetadata labelsMM = new MatrixMetadata(numRows, 1);
		// Script linReg =
		// ScriptFactory.dmlFromFile("scripts/algorithms/LinearRegCG.dml").in("X",
		// features, featuresMM)
		// .in("y", labels, labelsMM).out("beta_out");
		// ui.register(linReg);

		ui.start();
		ui.server.join();
	}

	public UI() {
	}

	public UI(MLContext mlContext) {
		this.mlContext = mlContext;
	}

	public void start() {
		try {
			if (status == Status.ON) {
				System.out.println("User interface already started (on port " + port + ").");
				return;
			}

			server = new Server(newPort);
			port = newPort;

			Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
			Velocity.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
			Velocity.init();

			Context context = new Context(server, "/");
			context.addServlet(new ServletHolder(new UIServlet(this, this.mlContext)), "/*");

			System.out.println("Starting user interface on port " + port + ".");
			server.start();
			status = Status.ON;
		} catch (Exception e) {
			throw new MLContextException("Error starting user interface on port: " + port, e);
		}
	}

	public void stop() {
		try {
			if (server == null) {
				status = Status.OFF;
				System.out.println("User interface has not been created.");
				return;
			}
			if (status == Status.OFF) {
				System.out.println("User interface is already stopped.");
				return;
			}
			if (server.isStopped()) {
				status = Status.OFF;
				System.out.println("User interface is already stopped.");
				return;
			} else if (server.isStopping()) {
				status = Status.OFF;
				System.out.println("User interface is stopping.");
				return;
			}

			System.out.println("Stopping user interface.");
			server.stop();
			status = Status.OFF;
		} catch (Exception e) {
			throw new MLContextException("Error stopping MLContext user interface", e);
		}
	}

	public String port() {
		if (port == newPort) {
			return "" + port;
		} else {
			return "current port: " + port + ", new port: " + newPort;
		}
	}

	public void port(int port) {
		if (status == Status.ON) {
			if (this.port != port) {
				System.out.println("Note that port change (from " + this.port + " to " + port
						+ ") will not show up until user interface is stopped and restarted.");
				this.newPort = port;
			} else {
				this.port = port;
				this.newPort = port;
				System.out.println("Port set to " + port);
			}
		} else {
			this.port = port;
			this.newPort = port;
			System.out.println("Port set to " + port);
		}
	}

	public void register(Script script) {
		this.script = script;
	}

	public Script getScript() {
		return script;
	}
}
