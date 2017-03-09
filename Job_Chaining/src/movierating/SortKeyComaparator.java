/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movierating;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author meenakshi
 */
public class SortKeyComaparator extends WritableComparator {

    public SortKeyComaparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        IntWritable key1 = (IntWritable) wc1;
        IntWritable key2 = (IntWritable) wc2;
        int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
        return result;
    }
}
