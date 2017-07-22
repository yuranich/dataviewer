import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;

/**
 * Iurii Samarin
 */
public class ParquetReader {
    public static void main(String[] args) throws IOException {
        Path path = new Path("browser_test.parquet");
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(path);
//        AvroParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
        GenericRecord next = null;
        while ((next = reader.read()) != null) {
            System.out.println(next);
        }
    }
}
