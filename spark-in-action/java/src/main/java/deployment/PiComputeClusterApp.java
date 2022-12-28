package deployment;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class PiComputeClusterApp {
    private static final long serialVersionUID = -1546L;
    private static long counter = 0;

    private final class DartMapper implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 38446L;

        @Override
        public Integer call(Row r) throws Exception {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            counter ++;
            if (counter % 1000 == 0) {
                System.out.println("" + counter + " operations done so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }

    private final class DartReducer implements ReduceFunction<Integer> {
        private static final long serialVersionUID = 12859L;

        @Override
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    }

    public static void main(String[] args) {
        PiComputeClusterApp app = new PiComputeClusterApp();
        app.start(10);
    }

    public void start(int slices) {
        int numberOfThrows = 100000 * slices;
        System.out.println("About to throw " + numberOfThrows + " darts");

        long t0 = System.currentTimeMillis();
        // The session resides on the cluster manager.
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkPi on a cluster")
                .master("spark://un:7077")
                .config("spark.executor.memory", "4g")
                .config("spark.jars", "jar/path")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        List<Integer> l = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            l.add(i);
        }

        // The first dataframe is created in the executor
        Dataset<Row> incrementalDf = spark.createDataset(l, Encoders.INT()).toDF();
        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        // This step is added to the DAG, which sits in the cluster manager
        Dataset<Integer> dartsDs = incrementalDf.map(new DartMapper(), Encoders.INT());
        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        // The result of the reduce operation is brought back to the application
        int dartsInCircle = dartsDs.reduce(new DartReducer());
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

        spark.stop();

    }
}
