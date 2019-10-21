import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {
	public static String dataset_name = "iyer.txt";
	public static String data_path = "/user/anhduc/kmeans_data/";
	public static String output_path = "/user/anhduc/kmeans_output/";
	public static int n_cluster = 10;
	public static int[] initial_points = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}; 
	
	public static ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
	
	
	// print centroids
	public static void printCentroids(ArrayList<ArrayList<Double>> centroids) {
		for (ArrayList<Double> centroid : centroids) {
			System.out.println(centroid);
		}
	} 
	
	// deep copy centroids
	public static ArrayList<ArrayList<Double>> deepCopyCentroids(ArrayList<ArrayList<Double>> centroids) {
		ArrayList<ArrayList<Double>> result = new ArrayList<>();
		for (int i = 0; i < centroids.size(); ++i) {
			ArrayList<Double> new_vec = new ArrayList<>();
			ArrayList<Double> vec = centroids.get(i);
			for (Double d : vec) {
				new_vec.add(d); 
			}
			result.add(new_vec);
		}
		return result;
	}
	
	// compute Euclidean distance between two vectors
	public static double distance(ArrayList<Double> vec1, ArrayList<Double> vec2) {
		double result = 0;
		for (int i = 0; i < vec1.size(); ++i) {
			double diff = (vec1.get(i) - vec2.get(i));
			result += diff * diff;		
		}
		return Math.sqrt(result);
	}
	
	// convert String to vector (ArrayList<Double>) 
	public static ArrayList<Double> convertDataRow(String line) {
		String[] arr = line.toString().split("\t");
		ArrayList<Double> vector = new ArrayList<>();
		for (int i = 2; i < arr.length; ++i) {
			vector.add(Double.parseDouble(arr[i]));
		}
		return vector;
	}
	
	// convert centroid in String format to vector (ArrayList<Double>) 
	public static ArrayList<Double> convertCentroid(String line) {
		String[] arr = line.toString().split("\t");
		ArrayList<Double> vector = new ArrayList<>();
		for (int i = 0; i < arr.length; ++i) {
			vector.add(Double.parseDouble(arr[i]));
		}
		return vector;
	}
	
	// sort the centroids in some particular order
	public static ArrayList<ArrayList<Double>> sort(ArrayList<ArrayList<Double>> centroids) {
		return centroids;
	}
	
	// check if previous centroids are exact the same with the current centroids
	public static boolean checkTheSame(ArrayList<ArrayList<Double>> prev_centroids, ArrayList<ArrayList<Double>> cur_centroids) {
		ArrayList<ArrayList<Double>> prev = sort(prev_centroids);
		ArrayList<ArrayList<Double>> cur = sort(cur_centroids);
		for (int i = 0; i < prev.size(); ++i) {
			ArrayList<Double> prev_vec = prev.get(i);
			ArrayList<Double> cur_vec = cur.get(i);
			for (int j = 0; j < prev_vec.size(); ++j) {
				if (Math.abs(prev_vec.get(j) - cur_vec.get(j)) > 0.0001) {
					return false;
				}
			}
		}
		return true;
	}
	
	// Mapper that perform Assignment step in Kmeans algorithm
	// Input Key: don't care
	// Input Value: data row (vector of double values separated by tab)
	// Output Key: centroid id
	// Output Value: data row (vector of double values separated by tab)
	
	public static class AssignmentMapper extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			ArrayList<Double> data_row = convertDataRow(value.toString());
			double minDist = Double.MAX_VALUE;
			int minClust = -1;
			for (int i = 0; i < centroids.size(); ++i) {
				double dist = distance(data_row, centroids.get(i));
				if (dist < minDist) {
					minDist = dist;
					minClust = i;
				}
			}
			context.write(new IntWritable(minClust), value);
		}
	}
	
	// Reducer that perform Update step in Kmeans algorithm
	// Input Key: cluster id (Integer)
	// Input Value: list of data row (each row is in Text format)
	// Output Key: centroid id
	// Output Value: data row (vector of double values separated by tab)
	public static class UpdateReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		@Override
		protected void reduce(IntWritable clustID, Iterable<Text> data_points, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			ArrayList<Double> newCentroid = null;
			int count = 0;
			for (Text data_row : data_points) {
				count++;
				if (newCentroid == null) {
					newCentroid = convertDataRow(data_row.toString());
				} else {
					ArrayList<Double> vector = convertDataRow(data_row.toString());
					for (int j = 0; j < vector.size(); ++j) {
						newCentroid.set(j, newCentroid.get(j) + vector.get(j));
					}
				}
				Text outputKey = new Text(data_row.toString().split("\t")[0]);
				context.write(outputKey, clustID);
			}
			for (int i = 0; i < newCentroid.size(); ++i) {
				newCentroid.set(i, newCentroid.get(i) / count);
			}
			centroids.set(clustID.get(), newCentroid);
		}
	}
	
	public static void main(String[] args)  {
		long startTime = System.currentTimeMillis();
		// TODO Auto-generated method stub
		/*
		HashMap<String, Integer> max_line = new HashMap<>();
		max_line.put("cho.txt", 386);
		max_line.put("iyer.txt", 517);
		max_line.put("new_dataset_1.txt", 150);
		max_line.put("new_dataset_2.txt", 6);
		HashMap<String, Integer> n_cluster = new HashMap<>();
		n_cluster.put("cho.txt", 5);
		n_cluster.put("iyer.txt", 10);
		n_cluster.put("new_dataset_1.txt", 3);
		n_cluster.put("new_dataset_2.txt", 2);*/
		
		Configuration conf = new Configuration();
		//conf.set("fs.default.name", "hdfs://localhost:9000");
		
		
		// get the initial position for centroids in kmeans from existing data points in the dataset
		/*
		ArrayList<Integer> permutation = new ArrayList<>();  
		for (int i = 0; i < max_line.get(dataset_name); ++i) {
			permutation.add(i);
		}
		Collections.shuffle(permutation);// construct the permutation of the row numbers 
		
		// initial positions for centroids will be first k values in the permutation
		List<Integer> initial_positions = permutation.subList(0, n_cluster.get(dataset_name));
		*/
		List<Integer> initial_positions = new ArrayList<>();
		for (int i = 0; i < initial_points.length; ++i) {
			initial_positions.add(initial_points[i]);
		}
		// sort the initial positions so that constructing initial centroids only need one pass over dataset
		Collections.sort(initial_positions);
		
		try {
			// set the path to dataset
			FileSystem fs = FileSystem.get(conf);
			Path dataset_path = new Path(data_path + dataset_name);
			
			// read data and constructing centroids
			BufferedReader input = new BufferedReader(new InputStreamReader(fs.open(dataset_path)));
			
			String line = input.readLine();
			int line_count = 0;
			int index = 0; // index to loop over all initial_position
			
			while (line != null) {
				if (line_count == initial_positions.get(index)) {
					// when the data row is actually the initial cluster centroid 
					centroids.add(convertDataRow(line));
					index++;
					//if (index >= n_cluster.get(dataset_name)) {
					if (index >= n_cluster) {
						// when all centroids have been initialized, finish initialization step
						break;
					}
				}
				line = input.readLine();
				line_count++;
			} 
			input.close();
			
			// run kmeans algorithm
			
			int iter = 0;
			ArrayList<ArrayList<Double>> prev_centroids = deepCopyCentroids(centroids);
			while (true) {
				iter++;
				System.out.println("Iteration: " + iter);
				// set up map reduce job
				Job job = Job.getInstance(conf, "KmeansMapReduce");
				job.setJarByClass(Kmeans.class);
				job.setMapperClass(AssignmentMapper.class);
				job.setReducerClass(UpdateReducer.class);
				
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.addInputPath(job, new Path(data_path + dataset_name));
				Path outputPath = new Path(output_path);
				
				// check if whether output path already exists
				if (fs.exists(outputPath)) {
					fs.delete(outputPath, true); // delete all files in that output path
				}
				
				FileOutputFormat.setOutputPath(job, outputPath); // set output path 
				// run map reduce
				job.waitForCompletion(true);
				System.out.println("Finish iteration " + iter);
				if (checkTheSame(prev_centroids, centroids) == true) {
					break;
				}
				prev_centroids = deepCopyCentroids(centroids);
			}
			
			// write centroids to file
			
			OutputStream os = fs.create(new Path(output_path + "centroids.txt"));
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			for (int i = 0; i < centroids.size(); ++i) {
				ArrayList<Double> vec = centroids.get(i);
				for (int j = 0; j < vec.size(); ++j) {
					String val = vec.get(j).toString();
					br.write(val);
					if (j == vec.size() - 1) {
						br.write("\n");
					} else {
						br.write("\t");
					}
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println("Total running time: " + elapsedTime);
	}
}
