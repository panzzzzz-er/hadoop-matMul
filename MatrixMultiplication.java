import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    // Mapper class
    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Read a line from the input file containing matrix data
            String line = value.toString();

            // Split the line into matrix element components
            String[] tokens = line.split(",");
            String matrixName = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);

            // Emit the elements to the reducer
            context.write(new Text(row + ","), new Text(matrixName + "," + col + "," + val));
        }
    }

    // Reducer class
    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] vectorA = new int[100]; // Assuming matrix A is 100x100
            int[] vectorB = new int[100]; // Assuming matrix B is 100x100

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                String matrixName = tokens[0];
                int col = Integer.parseInt(tokens[1]);
                int val = Integer.parseInt(tokens[2]);

                if (matrixName.equals("A")) {
                    vectorA[col] = val;
                } else {
                    vectorB[col] = val;
                }
            }

            // Compute the dot product of the two vectors
            int result = 0;
            for (int i = 0; i < 100; i++) { // Assuming 100 columns
                result += vectorA[i] * vectorB[i];
            }

            // Emit the result as (row, col) key and the computed value as the output
            String[] rowTokens = key.toString().split(",");
            int row = Integer.parseInt(rowTokens[0]);
            int col = Integer.parseInt(rowTokens[1]);
            context.write(new Text(row + "," + col), new IntWritable(result));
        }
    }

    // Main method to configure and run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MatrixMultiplication");
        job.setJarByClass(MatrixMultiplication.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
