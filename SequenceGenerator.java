import java.net.NetworkInterface;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Enumeration;
import org.apache.spark.SparkEnv;
// java 1.7 or older -- import org.threeten.bp.Instant;

/**
 * Distributed Sequence Generator.
 * Inspired by Twitter snowflake: https://github.com/twitter/snowflake/tree/snowflake-2010
 *
 * This class should be used as a Singleton.
 * Make sure that you create and reuse a Single instance of SequenceGenerator per node in your distributed system cluster.
 */
/**
 Using Executor ID instead of Node ID in Spark to avoid duplicates.
 */
public class SequenceGenerator {
    private static final int TOTAL_BITS = 64;
    private static final int EPOCH_BITS = 42;
    private static final int EXECUTOR_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;

    private static final int maxExecutorId = (int)(Math.pow(2, EXECUTOR_ID_BITS) - 1);
    private static final int maxSequence = (int)(Math.pow(2, SEQUENCE_BITS) - 1);

    // Custom Epoch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
    private static final long CUSTOM_EPOCH = 1420070400000L;

    private final int executorId;
    private static SequenceGenerator SINGLE_INSTANCE=null;

    private volatile long lastTimestamp = -1L;
    private volatile long sequence = 0L;

    // Let SequenceGenerator generate a executorId
    public SequenceGenerator() {
        this.executorId=createExecutorId();
    }
    
    // Singleton class
    public static SequenceGenerator getInstance() {
    	if(SINGLE_INSTANCE == null) {
    		synchronized(SequenceGenerator.class){
    			SINGLE_INSTANCE=new SequenceGenerator();
    		}
    	}
    	return SINGLE_INSTANCE;
    }
    
    // Generates nextId
    public synchronized long nextId() {
        long currentTimestamp = timestamp();

        if(currentTimestamp < lastTimestamp) {
            throw new IllegalStateException("Invalid System Clock!");
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & maxSequence;
            if(sequence == 0) {
                // Sequence Exhausted, wait till next millisecond.
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0;
        }

        lastTimestamp = currentTimestamp;

        long id = currentTimestamp << (TOTAL_BITS - EPOCH_BITS);
        id |= (executorId << (TOTAL_BITS - EPOCH_BITS - EXECUTOR_ID_BITS));
        id |= sequence;
        return id;
    }


    // Get current timestamp in milliseconds, adjust for the custom epoch.
    private static long timestamp() {
        return Instant.now().toEpochMilli() - CUSTOM_EPOCH;
    }

    // Block and wait till next millisecond
    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestamp();
        }
        return currentTimestamp;
    }

    // Generates executor id dynamically when the Spark application is submitted
    private int createExecutorId() {
    	int executorId;
    	try {
    		executorId=SparkEnv.get().executorId().hashCode();		
    	}
    	catch (Exception e) {
    		executorId = (new SecureRandom().nextInt());
    	}
        
    	executorId = executorId & maxExecutorId;
    	return executorId;
    }
}
