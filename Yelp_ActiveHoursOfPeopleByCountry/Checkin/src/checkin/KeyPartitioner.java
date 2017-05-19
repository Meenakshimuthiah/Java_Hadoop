/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package checkin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author meenakshi
 */
public class KeyPartitioner extends Partitioner<CompositeKey, Text>{
    
    @Override
    public int getPartition(CompositeKey key, Text value, int i) {
        return Math.abs(key.business_id.hashCode() & Integer.MAX_VALUE) % i;
    }
    
}
