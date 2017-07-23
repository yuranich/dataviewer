import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Iurii Samarin
 */
public class SimpleParquetReader {
    public static void main(String[] args) throws IOException {
        try(DirectoryStream<java.nio.file.Path> dis = Files.newDirectoryStream(Paths.get("results"))) {
            dis.forEach(path -> {
                if (path.toString().endsWith(".parquet")) {
//                    System.out.println("found file: " + path);
                    try {
                        Path p = new Path(path.toString());
//                        System.out.println("Hadoop file created: " + p.getName());
                        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(p);
                        GenericRecord next = null;
                        while ((next = reader.read()) != null) {
                            System.out.println(next);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });
        }
    }
}
