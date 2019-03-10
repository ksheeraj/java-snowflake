# java-snowflake
Distributed Unique ID Generator in Java inspired by Twitter Snowflake (Customized for Spark Cluster)

SequenceGenerator is used to generate Unique ID's (UUID's/GUID's) in a distributed Spark environment. It generates a 64-bit long value which is a combination of timestamp, executor id and a sequence counter.

RegisterUDF is used to register the Sequence Generator as a UDF so that it will be available in sqlContext.

To use the UDF in any java class, import the package - import static org.apache.spark.sql.functions.*;

For Spark<2.3, pass any dummy value as a parameter -> sqlContext.sql("select employeeId, generateUUID(1) from employees"); For Spark>2.3 -> sqlContext.sql("select employeeId, generateUUID() from employees");
