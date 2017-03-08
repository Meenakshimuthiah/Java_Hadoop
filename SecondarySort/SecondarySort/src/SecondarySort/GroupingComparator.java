/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author meenakshi
 */
public class GroupingComparator extends WritableComparator{
    public GroupingComparator(){
        super(CompositeKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2){
        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;
        
       return key1.symbol.compareTo(key2.symbol);
    }
    
    
}
