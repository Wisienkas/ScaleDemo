package dk.reibke.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wisienkas on 11-11-2015.
 */
public class Demo {

    // Will run the cluster on 4 cores locally
    SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    List<List<Integer>> map = new ArrayList<List<Integer>>();

    public Demo() {
        System.out.println("Starting demo...");

        initMap();

        StringBuilder sb = new StringBuilder();
        sb.append("       "); // 7 spaces
        for(int c = 0; c < map.size(); c++) sb.append(String.format("[c:%04d]", c));
        sb.append("\n");
        for(int r = 0; r < map.size(); r++) {
            sb.append(String.format("{r:%04d}", r));
            List<Integer> row = map.get(r);
            for(int c = 0; c < map.size(); c++) {
                sb.append(String.format("{  %02d  }", row.get(c)));
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    private void initMap() {
        // Creating inner lists
        for(int x = 0; x < 100; x++) {
            map.add(new ArrayList<Integer>());
        }
        // parallelize inner lists
        JavaRDD<List<Integer>> lists = sc.parallelize(map);
        map = lists.map(list -> {
            for(int i = 0; i < 100; i++) {
                list.add((int)(Math.random() * 99));
            }
            return list;
        }).collect();
    }
}
