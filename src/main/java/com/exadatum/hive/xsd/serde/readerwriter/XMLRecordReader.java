
package com.exadatum.hive.xsd.serde.readerwriter;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

public class XMLRecordReader implements RecordReader<LongWritable, Text> {

    public static final String START_TAG = "xmlinput.start";
    public static final String END_TAG = "xmlinput.end";

    private byte[] startTag;
    private byte[] endTag;
    private final long start;
    private final long end;
    private long pos;
    private InputStream inputstream;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private long recordStartPos;

  
    public XMLRecordReader(JobConf jobConf, InputStream inputstream, long start, long end) throws IOException {
        this.inputstream = inputstream;
        this.startTag = jobConf.get(START_TAG).getBytes("utf-8");
        this.endTag = jobConf.get(END_TAG).getBytes("utf-8");
        this.start = start;
        this.end = end;
        this.recordStartPos = this.start;
        this.pos = this.start;
    }


    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        if (readUntilMatch(this.startTag, false)) {
            this.recordStartPos = this.pos - this.startTag.length;
            try {
                this.buffer.write(this.startTag);
                if (readUntilMatch(this.endTag, true)) {
                    key.set(this.recordStartPos);
                    value.set(this.buffer.getData(), 0, this.buffer.getLength());
                    return true;
                }
            } finally {
                this.buffer.reset();
            }
        }

        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public void close() throws IOException {
        this.inputstream.close();
    }

    @Override
    public float getProgress() throws IOException {
        return ((float) (this.pos - this.start)) / ((float) (this.end - this.start));
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) {
        int i = 0;
        try {
            while (true) {
                int b = this.inputstream.read();
                ++this.pos;

                if (b == -1) {
                    return false;
                }
                if (withinBlock) {
                    this.buffer.write(b);
                }
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }
}
