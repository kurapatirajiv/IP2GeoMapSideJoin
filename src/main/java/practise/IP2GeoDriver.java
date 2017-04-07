package practise;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class IP2GeoDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        if(args.length!=3){
            System.out.print("Not Enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        DistributedCache.addCacheFile(new URI(args[0]+"input.txt"), job.getConfiguration());

        job.setJarByClass(IP2GeoDriver.class);
        job.setJobName("Map Side Join Application");

        // Set input paths for two mapper functions
        job.setMapperClass(IP2GeoMapper.class);
        job.setNumReduceTasks(0);


        FileInputFormat.addInputPath(job, new Path(args[1]));
        // Set the Output file Path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(args[2])))
            hdfs.delete(new Path(args[2]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
