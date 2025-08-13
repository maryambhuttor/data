// STEP 0: Imports
import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class QA_Mapper extends Mapper<Object, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1L);
    private static final Pattern RESO_RE = Pattern.compile("\\d+\\s*[xX]\\s*\\d+");
    private final HashSet<String> seenInThisSplit = new HashSet<>();

    private String sourceFile = ""; // images_metadata.csv | voice_metadata.csv | app_log.csv

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
        // STEP 1: Detect which input file this split is from
        InputSplit split = ctx.getInputSplit();
        if (split instanceof FileSplit) {
            sourceFile = ((FileSplit) split).getPath().getName().toLowerCase();
        }
    }

    @Override
    public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
        // STEP 2: Clean line and skip blanks
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // STEP 3: Skip header lines generically
        if (line.toLowerCase().startsWith("patientid")) return;

        // STEP 4: Split by comma, keep empties to catch missing fields
        String[] f = line.split(",", -1);

        // STEP 5: Route by source file and apply type-specific QA
        if (sourceFile.contains("images_metadata")) {
            // images: PatientID,ScanType,FileName,FileSize,Resolution
            if (f.length < 5) return;

            String patientId  = f[0].trim();
            String scanType   = f[1].trim();
            String fileName   = f[2].trim();
            String fileSize   = f[3].trim();
            String resolution = f[4].trim();
            String fileNameLc = fileName.toLowerCase();

            // Totals
            ctx.write(new Text("IMAGE_TOTAL"), ONE);

            // STEP 5a: Local duplicate detection (within this mapper split)
            if (!fileName.isEmpty()) {
                String dupKey = patientId + "|" + fileNameLc;
                if (seenInThisSplit.contains(dupKey)) {
                    ctx.write(new Text("DUPLICATE_LOCAL_TOTAL"), ONE);
                    ctx.write(new Text("DUPLICATE_LOCAL_IMAGE"), ONE);
                } else {
                    seenInThisSplit.add(dupKey);
                }
            }

            // STEP 5b: Per-scan-type tally
            if (!scanType.isEmpty()) ctx.write(new Text("IMAGE_BY_SCANTYPE|" + scanType), ONE);
            else ctx.write(new Text("IMAGE_MISSING_SCANTYPE"), ONE);

            // STEP 5c: FileSize QA
            if (fileSize.isEmpty()) {
                ctx.write(new Text("IMAGE_MISSING_FILESIZE"), ONE);
            } else {
                try {
                    long sz = Long.parseLong(fileSize);
                    if (sz <= 0) ctx.write(new Text("IMAGE_INVALID_FILESIZE_NONPOSITIVE"), ONE);
                } catch (NumberFormatException nfe) {
                    ctx.write(new Text("IMAGE_INVALID_FILESIZE_NAN"), ONE);
                }
            }

            // STEP 5d: Resolution QA
            if (resolution.isEmpty()) {
                ctx.write(new Text("IMAGE_MISSING_RESOLUTION"), ONE);
            } else if (!RESO_RE.matcher(resolution).matches()) {
                ctx.write(new Text("IMAGE_INVALID_RESOLUTION_FORMAT"), ONE);
            }

        } else if (sourceFile.contains("voice_metadata")) {
            // voice: PatientID,FileName,Duration_sec,SampleRate
            if (f.length < 4) return;

            String patientId  = f[0].trim();
            String fileName   = f[1].trim();
            String duration   = f[2].trim();
            String sampleRate = f[3].trim();
            String fileNameLc = fileName.toLowerCase();

            // Totals
            ctx.write(new Text("VOICE_TOTAL"), ONE);

            // STEP 5e: Local duplicate detection
            if (!fileName.isEmpty()) {
                String dupKey = patientId + "|" + fileNameLc;
                if (seenInThisSplit.contains(dupKey)) {
                    ctx.write(new Text("DUPLICATE_LOCAL_TOTAL"), ONE);
                    ctx.write(new Text("DUPLICATE_LOCAL_VOICE"), ONE);
                } else {
                    seenInThisSplit.add(dupKey);
                }
            }

            // STEP 5f: Duration QA
            if (duration.isEmpty()) {
                ctx.write(new Text("VOICE_MISSING_DURATION"), ONE);
            } else {
                try {
                    double d = Double.parseDouble(duration);
                    if (d <= 0) ctx.write(new Text("VOICE_INVALID_DURATION_NONPOSITIVE"), ONE);
                } catch (NumberFormatException nfe) {
                    ctx.write(new Text("VOICE_INVALID_DURATION_NAN"), ONE);
                }
            }

            // STEP 5g: Sample rate QA
            if (sampleRate.isEmpty()) {
                ctx.write(new Text("VOICE_MISSING_SAMPLERATE"), ONE);
            } else {
                try {
                    int sr = Integer.parseInt(sampleRate);
                    if (sr <= 0) ctx.write(new Text("VOICE_INVALID_SAMPLERATE_NONPOSITIVE"), ONE);
                } catch (NumberFormatException nfe) {
                    ctx.write(new Text("VOICE_INVALID_SAMPLERATE_NAN"), ONE);
                }
            }

        } else if (sourceFile.contains("app_log")) {
            // app log: PatientID,Timestamp,EventType,Value
            if (f.length < 4) return;

            ctx.write(new Text("CSV_TOTAL"), ONE);

            // STEP 5h: Required field checks
            checkRequired(ctx, f, 0, "CSV_MISSING_PATIENTID");
            checkRequired(ctx, f, 1, "CSV_MISSING_TIMESTAMP");
            checkRequired(ctx, f, 2, "CSV_MISSING_EVENTTYPE");
            checkRequired(ctx, f, 3, "CSV_MISSING_VALUE");

        } else {
            // Unknown file fallback
            ctx.write(new Text("UNKNOWN_TOTAL"), ONE);
        }
    }

    // STEP 6: Helper for required fields
    private void checkRequired(Context ctx, String[] f, int idx, String metric)
            throws IOException, InterruptedException {
        if (f.length <= idx || f[idx].trim().isEmpty()) {
            ctx.write(new Text(metric), ONE);
        }
    }
}
