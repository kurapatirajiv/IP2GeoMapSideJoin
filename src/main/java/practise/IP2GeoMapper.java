package practise;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by Rajiv on 3/20/17.
 */
public class IP2GeoMapper extends Mapper<LongWritable, Text, Text, Text> {


    public HashMap<String, String> locationMap = new HashMap<String, String>();


    public void setup(Mapper.Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();

        // Load the distributed cache into
        Scanner scanner = new Scanner(new File(cacheFiles[0].getPath()));
        while (scanner.hasNextLine()) {
            String[] inputLine = scanner.nextLine().split(",");
            locationMap.put(inputLine[0], inputLine[1]);
        }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String data[] = value.toString().split(",");
        String result = "UnKnown";

        if (locationMap.containsKey(data[2])) {
            result = locationMap.get(data[2]);
        }

        context.write(value, new Text(result));
    }
}
