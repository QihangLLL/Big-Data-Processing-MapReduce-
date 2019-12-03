package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.StringTokenizer;

import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.edu.rmit.bdp.clustering.model.DataPoint;

/**
 * K-means algorithm in mapReduce
 * <p>
 *
 * Terminology explained: - DataPoint: A dataPoint is a point in 2 dimensional
 * space. we can have as many as points we want, and we are going to group those
 * points that are similar( near) to each other. - cluster: A cluster is a group
 * of dataPoints that are near to each other. - Centroid: A centroid is the
 * center point( not exactly, but you can think this way at first) of the
 * cluster.
 *
 * Files involved: - data.seq: It contains all the data points. Each chunk
 * consists of a key( a dummy centroid) and a value(data point). - centroid.seq:
 * It contains all the centroids with random initial values. Each chunk consists
 * of a key( centroid) and a value( a dummy int) - depth_*.seq: These are a set
 * of directories( depth_1.seq, depth_2.seq, depth_3.seq ... ), each of the
 * directory will contain the result of one job. Note that the algorithm works
 * iteratively. It will keep creating and executing the job before all the
 * centroid converges. each of these directory contains files which is produced
 * by reducer of previous round, and it is going to be fed to the mapper of next
 * round. Note, these files are binary files, and they follow certain protocals
 * so that they can be serialized and deserialized by SequenceFileOutputFormat
 * and SequenceFileInputFormat
 *
 * This is an high level demonstration of how this works:
 *
 * - We generate some data points and centroids, and write them to data.seq and
 * cen.seq respectively. We use SequenceFile.Writer so that the data could be
 * deserialize easily.
 *
 * - We start our first job, and feed data.seq to it, the output of reducer
 * should be in depth_1.seq. cen.seq file is also updated in reducer#cleanUp. -
 * From our second job, we keep generating new job and feed it with previous
 * job's output( depth_1.seq/ in this case), until all centroids converge.
 *
 */
public class KMeansClusteringJob {

	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
	private static int fileLength = 0;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
		
		//Set paths by using command line
		//As the assignment's specification asked, the paths should not be hard-coded
	    String inital_position = new String(args[0]);
	    Path inputFile = new Path(args[1]);
	    Path outputFile = new Path(args[2]);
	    String KValue = new String(args[3]);

		int k = Integer.parseInt(KValue);

		Path PointDataPath = new Path(inital_position + "data.seq");
		Path centroidDataPath = new Path(inital_position + "centroid.seq");

		conf.set("centroid.path", centroidDataPath.toString());
		conf.set("dirPath", inital_position);

		Path outputDir = new Path(inital_position + "depth_1");

		Job job = Job.getInstance(conf);
		job.setJobName("KMeans Clustering");

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		FileInputFormat.addInputPath(job, PointDataPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		if (fs.exists(centroidDataPath)) {
			fs.delete(centroidDataPath, true);
		}

		if (fs.exists(PointDataPath)) {
			fs.delete(PointDataPath, true);
		}
		//add the input file to the path
		FileInputFormat.addInputPath(job, inputFile);
		//generate Datapoints and centroids
		generateDataPoints(conf, inputFile, PointDataPath, fs);
		generateCentroid(conf, inputFile, centroidDataPath, fs, k);
		//Give a reference to read data.seq
		FileInputFormat.setInputPaths(job, PointDataPath);

        //set the the number of working reducer 
		job.setNumReduceTasks(1);
		//set the output path
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//set classes using to write file
		job.setInputFormatClass(FileInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Centroid.class);
		job.setOutputValueClass(DataPoint.class);

		job.waitForCompletion(true);

		//start iteration
		long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter>0) {
			conf = new Configuration();
			conf.set("centroid.path", centroidDataPath.toString());
			conf.set("num.iteration", iteration + "");
			conf.set("dirPath", inital_position);
			job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			PointDataPath = new Path(inital_position + "depth_" + (iteration - 1) + "/");
			outputDir = new Path(inital_position + "depth_" + iteration);

			FileInputFormat.addInputPath(job, PointDataPath);
			if (fs.exists(outputDir))
				fs.delete(outputDir, true);

			FileOutputFormat.setOutputPath(job, outputDir);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Centroid.class);
			job.setOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);

			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path(inital_position + "/" + "depth_" + (iteration - 1) + "/");

		//check if the output file is exit
		//if so, delete
		if (fs.exists(outputFile)) {
			fs.delete(outputFile, true);// 
		}
		//set output file's path
		FileOutputFormat.setOutputPath(job, outputFile);

		FileStatus[] stati = fs.listStatus(result);

		//write the result into output file
		FSDataOutputStream outStream = fs.create(outputFile);
		for (FileStatus status : stati) {
			if (!status.isDirectory()) {
				Path path = status.getPath();
				if (!path.getName().equals("_SUCCESS")) {
					LOG.info("FOUND " + path.toString());
					try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
						Centroid key = new Centroid();
						DataPoint v = new DataPoint();
						while (reader.next(key, v)) {
							// LOG.info(key + " / " + v);
							//Write one data center fallowing a datapoint as one record into outout file
							double[] centers = key.getCenterVector().toArray();
							double[] dataPoints = v.getVector().toArray();
							String outPut = centers[0] + "," + centers[1] + "," + dataPoints[0] + "," + dataPoints[1] + "\n";
							outStream.write(outPut.getBytes("UTF-8"));
						}
						
					}
				}
			}
		}
		//close the outStream after all records have been written
		outStream.close();
	}

	@SuppressWarnings("deprecation")
	public static void generateDataPoints(Configuration conf, Path input, Path in, FileSystem fs) throws IOException {

		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Centroid.class,
				DataPoint.class)) {
            //Create a inputStream to read the input file
			FSDataInputStream inStream = fs.open(input);
			BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
			String tmp = "";
			while ((tmp = reader.readLine()) != null) {
				String[] split = tmp.split(",");
				double pickup_longitude = Double.parseDouble(split[0]);
				double pickup_latitude = Double.parseDouble(split[1]);
				//Generate a new datapoint which uses longitude and latitude as x,y 
				//The key pair here should be the datacenter as key, the datapoint as value
				dataWriter.append(new Centroid(new DataPoint(0, 0)), new DataPoint(pickup_longitude, pickup_latitude));
				fileLength++;
			}
			reader.close();
			inStream.close();
		}

	}


	@SuppressWarnings("deprecation")
	public static void generateCentroid(Configuration conf, Path input, Path center, FileSystem fs, int k)
			throws IOException {

		ArrayList<Integer> LineList = new ArrayList<Integer>();

      //Depended on the K, generate k number of random lineNumber  
		for (int i = 0; i < k; i++) {
			int randomNum = (int) (Math.random() * fileLength);
			LineList.add(randomNum);
		}

		//Read the input file and get the longitude and latitude 
		//depending on the lineNumber
		//then write them into centorid.seq
		try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
				IntWritable.class)) {
			final IntWritable value = new IntWritable(0);
			FSDataInputStream inStream = fs.open(input);
			BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
			String tmp = "";
			int lineNum = 0;
			while ((tmp = reader.readLine()) != null) {
				String[] split = tmp.split(",");
				double pickup_longitude = Double.parseDouble(split[0]);
				double pickup_latitude = Double.parseDouble(split[1]);
				//If the line number is same as the one in the list
				//Write the center into centroid.seq
				if (LineList.contains(lineNum)) {

//					System.out.println(pickup_longitude);
//					System.out.println(pickup_latitude);

					centerWriter.append(new Centroid(new DataPoint(pickup_longitude, pickup_latitude)), value);
				}
				lineNum++;
			}
			reader.close();
			inStream.close();
		}
	}

}
