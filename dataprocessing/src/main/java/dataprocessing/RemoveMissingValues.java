package dataprocessing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RemoveMissingValues {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Remove Missing Values")
            .master("local[*]") 
            .getOrCreate();
        
        String inputPath = "D://datasets//new18.csv";
        String outputPath = "D://datasets//new20.csv";

        Dataset<Row> df = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(inputPath);

        Dataset<Row> cleanedDf = df.na().drop();  


        cleanedDf.coalesce(1)
        .write()
        .option("header", "true")
        .csv(outputPath);

        spark.stop();
    }
}
