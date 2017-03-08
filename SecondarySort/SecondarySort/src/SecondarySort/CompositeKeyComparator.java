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
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;

        int symbolCmp = key1.symbol.compareTo(key2.symbol);
        if (symbolCmp != 0) {
            return symbolCmp;
        } else {
            int stockCmp = -1 * Integer.compare(key1.stock_vol, key2.stock_vol);
                return stockCmp;
        }
    }
}
