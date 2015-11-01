package hadoop.custom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Based on https://hadoopi.wordpress.com/2013/05/31/custom-recordreader-processing-string-pattern-delimited-records/
 */
public class PatternInputFormat
        extends FileInputFormat<LongWritable,Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split,
            TaskAttemptContext context)
            throws IOException,
            InterruptedException {

        return new PatternRecordReader();
    }

}