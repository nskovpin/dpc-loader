package ru.at_consulting.bigdata.dpc.cluster.staging;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class TestDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        System.out.println("STAGE1");

        System.out.println("Testing yoda:" + DateTime.now());
        System.out.println("Testing commons");
        Pair<String, String> pair = Pair.of("left","right");
        System.out.println("Value:" + pair.getLeft());
        System.out.println("Testing lombok");


        FileSystem hdfs = FileSystem.get(getConf());
        Path path = new Path("/user/nskovpin/json/second");
        InputStream inputStream = hdfs.open(path);
        System.out.println("JSON:");
        System.out.println(IOUtils.toString(inputStream));


        Path pt=new Path("/user/nskovpin/json/first");
        FileSystem fs = FileSystem.get(getConf());
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));

        // TO append data to a file, use fs.append(Path f)
        String line;
        line="Disha Dishu Daasha";
        System.out.println(line);
        br.write(line);
        br.close();

//        Job job = Job.getInstance(getConf(), "lib.test.job");
//        job.setJarByClass(TestDriver.class);
//
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(Text.class);
//        job.setReducerClass(Reducer.class);
//        job.setMapperClass(MMapper.class);
//
//        FileInputFormat.addInputPath(job, path);
//
//        job.setNumReduceTasks(1);
//
//        FileOutputFormat.setOutputPath(job, new Path("/user/nskovpin/json/third"));
//
//
//
//        boolean result = job.waitForCompletion(true);
//        return result ? 0 : 1;
        return 0;
    }

}
