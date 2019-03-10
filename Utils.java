import java.io.Serializable;


public class Utils implements Serializable  { 
 
 private static final long serialVersionUID = 1L;
 
 public static synchronized long generateUUID()
	{
		long UUID=SequenceGenerator.getInstance().nextId();
		return UUID;
	}
 
 }
