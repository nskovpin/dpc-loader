package ru.at_consulting.bigdata.dpc.cluster.system;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

/**
 * Created by NSkovpin on 06.03.2017.
 */
@Getter
@Setter
@ToString
public class ClusterProperties {
    public static final String WORK = "work";
    public static final String DATA = "data";

    public String projectName;
    public String hdfsJsonPath;
    public String hdfsOutputDir;
    public String hdfsWorkDir;
    public String hdfsDataDir;

    public enum PARAM_NAMES {
        PROJECT_NAME,
        HDFS_JSON_PATH,
        HDFS_OUTPUT_DIR,
    }

    public ClusterProperties(Configuration configuration){
        this.projectName = configuration.get(PARAM_NAMES.PROJECT_NAME.name());
        this.hdfsJsonPath = configuration.get(PARAM_NAMES.HDFS_JSON_PATH.name());
        this.hdfsOutputDir = configuration.get(PARAM_NAMES.HDFS_OUTPUT_DIR.name());
        this.hdfsWorkDir = hdfsOutputDir + File.separator + WORK;
        this.hdfsDataDir = hdfsOutputDir + File.separator + DATA;
    }

    public ClusterProperties(String[] args){
        for (String argument: args){
            String[] keyValue = argument.split("=", -1);
            PARAM_NAMES parameter = PARAM_NAMES.valueOf(keyValue[0]);
            switch (parameter){
                case PROJECT_NAME:{
                    this.projectName =  keyValue[1];
                }
                case HDFS_JSON_PATH:{
                    this.hdfsJsonPath = keyValue[1];
                }
                case HDFS_OUTPUT_DIR:{
                    this.hdfsOutputDir = keyValue[1];
                    this.hdfsWorkDir = hdfsOutputDir + File.separator + WORK;
                    this.hdfsDataDir = hdfsOutputDir + File.separator + DATA;
                }
            }
        }
    }
}
