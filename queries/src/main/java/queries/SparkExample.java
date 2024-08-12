package queries;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkExample {
    public static void main(String[] args) {
     
        SparkSession spark = SparkSession.builder()
            .appName("CSV Query Example")
            .getOrCreate();

       
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("D:/processed_data.csv"); // Use forward slashes or double backslashes

     
        df.createOrReplaceTempView("csv_table");

       
     Dataset<Row> result = spark.sql("SELECT * FROM csv_table WHERE Product_Ratings > 3");    
     result.write()
         .option("header", "true")
           .csv("D:/datasets/query1.csv");
        

        // Query 2: Select rows where Discount (%) > 10
    Dataset<Row> result3 = spark.sql("SELECT * FROM csv_table WHERE `Discount (%)` > 10");
    result3.write()
         .option("header", "true")
          .csv("D:/datasets/query2.csv");


        // Query 3: 
      Dataset<Row> result4 = spark.sql("SELECT * FROM csv_table WHERE Sale_Price BETWEEN 100 AND 300");

        result4.write()
            .option("header", "true")
           .csv("D:/datasets/query3.csv");

        // Query 4: Select top 5 rows with highest Sale_Price
       Dataset<Row> result5 = spark.sql("SELECT * FROM csv_table ORDER BY Sale_Price DESC LIMIT 5");
       result5.write()
           .option("header", "true")
           .csv("D:/datasets/query4.csv");
       
        Dataset<Row> result6 = spark.sql("SELECT * FROM csv_table WHERE Quantity LIKE '%Multipack%'");

        result6.write()
        .option("header", "true")
       .csv("D:/datasets/query5.csv");
    
        // Stop the SparkSession
    
    spark.stop();
    }
    }
      
