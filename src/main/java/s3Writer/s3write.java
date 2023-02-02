package s3Writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;


public class s3write {
    public static void write2Disk(String localFilePath, Iterator<String> iter) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(localFilePath));
        iter.forEachRemaining(e -> {
            try {
                writer.write(e);
                writer.newLine();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        writer.flush();
    }
}