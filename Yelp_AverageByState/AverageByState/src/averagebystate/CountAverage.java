/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagebystate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author meenakshi
 */
public class CountAverage implements Writable{
    
    
    private int count;
    private double average;
    
    public CountAverage() {
        
    }
    
    public CountAverage( int count, double average) {
       
        this.count = count;
        this.average = average;
    }

    

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }
    
    

    @Override
    public void write(DataOutput d) throws IOException {
        
        d.writeInt(count);
        d.writeDouble(average);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       
        count = di.readInt();
        average = di.readDouble();
    }
    
    @Override
    public String toString() {
        return (new StringBuilder().append(average).toString());
    }
}
