/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author meenakshi
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    int stock_vol;
    String symbol;
    

    public CompositeKey() {

    }

    public CompositeKey(String symbol, int stock_vol) {
        super();
        
    }

    public int getStock_vol() {
        return stock_vol;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setStock_vol(int stock_vol) {
        this.stock_vol = stock_vol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    

    
    public void set(String symbol, int stock_vol,double adj) {
        this.stock_vol = stock_vol;
        this.symbol = symbol;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(stock_vol);
        d.writeUTF(symbol);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        stock_vol = di.readInt();
        symbol = di.readUTF();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int symbolCmp = symbol.compareTo(o.symbol);
        if (symbolCmp != 0) {
            return symbolCmp;
        } 
        else{
            int stock_cmp = Integer.compare(stock_vol, o.stock_vol);
                return stock_cmp;
        }   
        
    }

    @Override
     public String toString()
    {
        return (new StringBuilder().append(symbol).append("\t").append(stock_vol).toString());
    }
}
