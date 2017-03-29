package io.adrianosb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author adriano
 */
public class TuplaWritable implements Writable {

    private Text path;
    private IntWritable sum;

    public TuplaWritable() {
        this.path = new Text();
        this.sum = new IntWritable();
    }
    
    public TuplaWritable(Text path, IntWritable sum) {
        this.path = path;
        this.sum = sum;
    }

    public TuplaWritable(String path, int sum) {
        this.path = new Text(path);
        this.sum = new IntWritable(sum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        path.write(out);
        sum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path.readFields(in);
        sum.readFields(in);
    }

    public Text getPath() {
        return path;
    }

    public void setPath(Text path) {
        this.path = path;
    }

    public IntWritable getSum() {
        return sum;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + Objects.hashCode(this.path);
        hash = 67 * hash + Objects.hashCode(this.sum);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TuplaWritable other = (TuplaWritable) obj;
        if (!Objects.equals(this.path, other.path)) {
            return false;
        }
        return Objects.equals(this.sum, other.sum);
    }

    @Override
    public String toString() {
        return path + (StringUtils.isNotBlank(sum.toString())?": " + sum + ';':"");
    }

}
