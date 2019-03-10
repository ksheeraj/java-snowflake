import java.io.Serializable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

public class RegisterUDF implements Serializable{
  
  public void registerUDF(SparkContext context, SQLContext sqlContext){ 
    
     final Utils util=new Utils();
     
  // For Spark < 2.3, we cannot register UDF with no parameters, so passing a dummy value. 
  	 sqlContext.udf().register("generateUUID", new UDF1<Integer, Long>() {
	        private static final long serialVersionUID = 1L;
	         
	        @Override
	        public Long call(Integer value) throws Exception {
	        	{
	                    return util.generateUUID();
	        	}
	        }
	  }, DataTypes.LongType); 
    
  // For Spark > 2.3 onwards, we can register UDF's with no parameters.
    
     sqlContext.udf().register("generateUUID", new UDF0<Long>() {
	        private static final long serialVersionUID = 1L;
	         
	        @Override
	        public Long call() throws Exception {
	        	{
	                    return util.generateUUID();
	        	}
	        }
	  }, DataTypes.LongType); 
    
  }
}
