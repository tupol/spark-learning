package hadoop.custom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Based on https://hadoopi.wordpress.com/2013/05/31/custom-recordreader-processing-string-pattern-delimited-records/
 */
public class PatternRecordReader
        extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(
            PatternRecordReader.class);

    private LineReader in;
    private final static Text OPEN_MARKER = new Text(",{");
    private final static Text CLOSE_MARKER = new Text("}");
    private final static Text SEPARATOR = new Text(";");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private int maxLengthRecord;

    private long start;
    private long pos;
    private long end;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private Text carryValue = new Text();

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context)
            throws IOException, InterruptedException {

        Configuration job = context.getConfiguration();
        this.delimiterRegex = job.get("record.delimiter.regex");
        this.maxLengthRecord = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        delimiterPattern = Pattern.compile(delimiterRegex);


        // This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) genericSplit;
        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();

        // Retrieve file containing Split "S"
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        // If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }

        in = new LineReader(fileIn, job);

        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE,
                            end - start));
        }

        // Position is the actual start
        this.pos = start;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        // Current offset is the key
        key.set(pos);

        int newSize = 0;

        // Make sure we get at least one record that starts in this Split
        while (pos < end) {

            // Read first line and store its content to "value"
            newSize = readNext(value, maxLengthRecord,
                    Math.max((int) Math.min(
                                    Integer.MAX_VALUE, end - pos),
                            maxLengthRecord));

            // No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
            if (newSize == 0) {
                break;
            }

            // Line is read, new position is set
            pos += newSize;

            // Line is lower than Maximum record line size
            // break and return true (found key / value)
            if (newSize < maxLengthRecord) {
                break;
            }

            // Line is too long
            // Try again with position = position + line offset,
            // i.e. ignore line and go to next one
            // TODO: Shouldn't it be LOG.error instead ??
            LOG.info("Skipped line of size " +
                    newSize + " at pos "
                    + (pos - newSize));
        }


        if (newSize == 0) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        } else {
            // Tell Hadoop a new line has been found
            // key / value will be retrieved by
            // getCurrentKey getCurrentValue methods
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    private int readNext(Text text,
                         int maxLineLength,
                         int maxBytesToConsume)
            throws IOException {

        int offset = 0;

        text.clear();

        Text tmp = new Text();

        for (int i = 0; i < maxBytesToConsume; i++) {

            int offsetTmp = in.readLine(
                    tmp,
                    maxLineLength,
                    maxBytesToConsume);
            offset += offsetTmp;
            Matcher m = delimiterPattern.matcher(tmp.toString());

            // End of File
            if (offsetTmp == 0) {
                break;
            }

            if (m.matches()) {
                if (i != 0) {
                    text.append(CLOSE_MARKER.getBytes(), 0, CLOSE_MARKER.getLength());
                } else {
                    text.append(tmp.getBytes(), 0, tmp.getLength());
                }
                // We need to carry the match, in case we have no =n matching lines next
                carryValue.clear();
                carryValue.append(tmp.getBytes(), 0, tmp.getLength());
                break;
            } else {
                if (i == 0) {
                    // If there was a value carried then we initialize the current value with the carried one
                    if (carryValue.getLength() > 0) {
                        text.append(carryValue.getBytes(), 0, carryValue.getLength());
                    }
                    text.append(OPEN_MARKER.getBytes(), 0, OPEN_MARKER.getLength());
                } else {
                    text.append(SEPARATOR.getBytes(), 0, SEPARATOR.getLength());
                }
                // Append value to record
                text.append(tmp.getBytes(), 0, tmp.getLength());
            }

        }
        return offset;
    }
}
