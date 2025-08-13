// STEP 0: Imports
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QA_Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> vals, Context ctx)
            throws IOException, InterruptedException {

        // STEP 1: Aggregate counts
        long sum = 0L;
        for (LongWritable v : vals) sum += v.get();

        // STEP 2: Emit final metric
        ctx.write(key, new LongWritable(sum));
    }
}
