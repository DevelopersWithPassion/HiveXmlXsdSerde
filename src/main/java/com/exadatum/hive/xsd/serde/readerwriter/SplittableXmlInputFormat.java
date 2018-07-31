
package com.exadatum.hive.xsd.serde.readerwriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class SplittableXmlInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {

        InputStream inputStream = null;
        try {
            inputStream = getInputStream(job, (FileSplit) inputSplit);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        long start = ((FileSplit) inputSplit).getStart();
        long end = start + inputSplit.getLength();

        return new XMLRecordReader(job, inputStream, start, end);
    }

    private InputStream getInputStream(JobConf jobConf, FileSplit split) throws IOException, ClassNotFoundException {
        FSDataInputStream fsin = null;

        // open the file and seek to the start of the split
        long splitStart = split.getStart();
        long splitEnd = splitStart + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(split.getPath());
        fsin.seek(splitStart);

        Configuration conf = new Configuration();
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = compressionCodecFactory.getCodec(split.getPath());
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
            return ((SplittableCompressionCodec) codec).createInputStream(fsin,
                decompressor,
                splitStart,
                splitEnd,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        } else {
            return codec.createInputStream(fsin, decompressor);
        }
    }
}