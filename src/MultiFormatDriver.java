// STEP 0: Imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiFormatDriver {

    public static void main(String[] args) throws Exception {

        // STEP 1: Usage check
        if (args.length < 2) {
            System.err.println("Usage: MultiFormatDriver <input_dir> <output_dir>");
            System.exit(1);
        }

        // STEP 2: Configure job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MultiFormatQAEnhanced");

        // STEP 3: Wire classes
        job.setJarByClass(MultiFormatDriver.class);
        job.setMapperClass(QA_Mapper.class);
        job.setReducerClass(QA_Reducer.class);

        // STEP 4: Set I/O types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // STEP 5: Input/Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));   // e.g., data
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // e.g., output

        // STEP 6: Run
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
