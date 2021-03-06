/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package checkin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author meenakshi
 */
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;

        int result = key1.business_id.compareTo(key2.business_id);
        if (result == 0) {
            result = -1*Integer.compare(key1.check_in, key2.check_in);
        }
        return result;
    }
}
