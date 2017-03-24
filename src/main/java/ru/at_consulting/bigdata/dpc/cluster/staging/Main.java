package ru.at_consulting.bigdata.dpc.cluster.staging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class Main extends Configured implements Tool {

    public static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        LOG.info("!START DMAIN!");

        return  ToolRunner.run(conf, new TestDriver(), strings);
    }
}