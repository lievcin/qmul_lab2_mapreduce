package bdp.stock;

import java.io.*;
import org.apache.hadoop.io.*;

public class DblDblPair implements WritableComparable<DblDblPair> {
  private DoubleWritable first;
  private DoubleWritable second;

  public DblDblPair() {
    set(new DoubleWritable(), new DoubleWritable());
  }

  public DblDblPair(int first, double second) {
    set(new DoubleWritable(first), new DoubleWritable(second));
  }


  public void set(DoubleWritable first, DoubleWritable second) {
    this.first = first;
    this.second = second;
  }

  public DoubleWritable getFirst() {
    return first;
  }
  public DoubleWritable getSecond() {
    return second;
  }
  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DblDblPair) {
      DblDblPair tp = (DblDblPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }
  @Override
  public String toString() {
    return first + "\t" + second;
  }

  @Override
  public int compareTo(DblDblPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
}