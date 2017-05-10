package ru.at_consulting.bigdata.dpc.cluster.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class HdfsWriter {

    public static BufferedWriter createWriter(String path, Configuration configuration, boolean append) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Path hdfsPath = new Path(path);
        if (!append) {
            checkExist(hdfsPath, fs);
            return new BufferedWriter(new OutputStreamWriter(fs.create(hdfsPath, true)));
        } else {
            return new BufferedWriter(new OutputStreamWriter(fs.append(hdfsPath)));
        }
    }

    public static BufferedWriter createWriter(String path, FileSystem fs, boolean append) throws IOException {
        Path hdfsPath = new Path(path);
        if (!append) {
            checkExist(hdfsPath, fs);
            return new BufferedWriter(new OutputStreamWriter(fs.create(hdfsPath, true)));
        } else {
            return new BufferedWriter(new OutputStreamWriter(fs.append(hdfsPath)));
        }
    }

    public static void writeLine(BufferedWriter bufferedWriter, String line) throws IOException {
        bufferedWriter.write(line);
        bufferedWriter.newLine();
    }

    public static void writeLines(BufferedWriter bufferedWriter, Set<String> lines) throws IOException {
        for (String line : lines) {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }
    }

    public static void closeWriter(BufferedWriter bufferedWriter) throws IOException {
        bufferedWriter.close();
    }

    private static void checkExist(Path path, FileSystem fileSystem) throws IOException {
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static Set<String> readLogger(Path path, FileSystem fileSystem) throws IOException {
        Set<String> linesSet = new HashSet<>();
        if(fileSystem.exists(path)){
            BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String line;
            line=br.readLine();
            while (line != null){
                line=br.readLine();
                linesSet.add(line);
            }
        }
        return linesSet;
    }
}
