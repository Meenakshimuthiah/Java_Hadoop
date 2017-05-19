/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package checkin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author meenakshi
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    int check_in;
    String business_id;
    
    

    public CompositeKey() {

    }

    public CompositeKey(String user_id, int check_in) {
        this.business_id = user_id;
        this.check_in = check_in;
        
    }

    public int getFans() {
        return check_in;
    }

    public void setFans(int fans) {
        this.check_in = fans;
    }

    public String getUser_id() {
        return business_id;
    }

    public void setUser_id(String user_id) {
        this.business_id = user_id;
    }

    


    

    
    public void set(String user_id, int fans) {
        this.check_in = fans;
        this.business_id = user_id;
        
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(check_in);
        d.writeUTF(business_id);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        check_in = di.readInt();
        business_id = di.readUTF();
        
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = business_id.compareTo(o.business_id);
        if (result == 0) {
            result = -1*Integer.compare(check_in, o.check_in);
        }   
        return result;
    }

    @Override
     public String toString()
    {
        return (new StringBuilder().append(business_id).toString());
    }
}
