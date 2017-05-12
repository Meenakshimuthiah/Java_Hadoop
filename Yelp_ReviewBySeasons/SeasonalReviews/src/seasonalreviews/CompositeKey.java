/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package seasonalreviews;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;



/**
 *
 * @author meenakshi
 */
public class CompositeKey implements Writable,WritableComparable<CompositeKey>{
    String business_id;
    String season;

    public CompositeKey() {
    }

    public CompositeKey(String business_id, String season) {
        this.business_id = business_id;
        this.season = season;
    }

    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(season);
        d.writeUTF(business_id);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        season = di.readUTF();
        business_id = di.readUTF();
   }
    
    @Override
    public String toString(){
        return (new StringBuilder().append(business_id).append("\t").append(season).toString());
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = business_id.compareTo(o.business_id);
        if(result ==0){
            result = season.compareTo(o.season);
        }
        return result;
    }
    
}
