package ru.at_consulting.bigdata.dpc.cluster.loader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by NSkovpin on 04.05.2017.
 */
public class CustomPathFilter implements PathFilter {
    private static final Logger LOGGER = Logger.getLogger(CustomPathFilter.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");

    private LocalDateTime timeKey;
    private Set<String> alreadySeen;
    private Set<String> newDates = new HashSet<>();

    public CustomPathFilter(LocalDateTime localDateTime, Set<String> alreadySeen) {
        this.timeKey = localDateTime;
        this.alreadySeen = alreadySeen;
    }

    @Override
    public boolean accept(Path path) {
        if (path != null) {
            String pathStr = path.toString();
            String dateFromPath = getDateFromPaths(path);
            if (dateFromPath.length() > 0) {
                LocalDateTime pathTime;
                try {
                    pathTime = LocalDateTime.parse(dateFromPath, DATE_TIME_FORMATTER);
                } catch (Exception e) {
                    System.out.println("Impossible to parse:" + dateFromPath);
                    LOGGER.info("Impossible to parse:" + dateFromPath);
                    return false;
                }
                if (!pathTime.isAfter(timeKey)) {
                    if (!alreadySeen.contains(dateFromPath)) {
                        this.newDates.add(dateFromPath);
                        return true;
                    } else {
                        System.out.println("Path has already seen:" + pathStr);
                        LOGGER.info("Path has already seen:" + pathStr);
                    }
                } else {
                    System.out.println("Path is after timeKey. TimeKey:" + timeKey.toString("yyyyMMdd") + "; Path:" + pathStr);
                    LOGGER.info("Path is after timeKey. TimeKey:" + timeKey.toString("yyyyMMdd") + "; Path:" + pathStr);
                }
            } else {
                System.out.println("Path doesn't contain date:" + pathStr);
                LOGGER.info("Path doesn't contain date:" + pathStr);
            }
        }
        return false;
    }

    private String getDateFromPaths(Path path) {
        String day = "";
        String month = "";
        String year = "";
        Path pathParentDD = path.getParent();
        if (pathParentDD != null) {
            day = pathParentDD.getName();
            Path pathParentMM = pathParentDD.getParent();
            if (pathParentMM != null) {
                month = pathParentMM.getName();
                Path pathParentYYYY = pathParentMM.getParent();
                if (pathParentYYYY != null) {
                    year = pathParentYYYY.getName();
                }
            }
        }
        return year + month + day;
    }

    public Set<String> getNewDates() {
        return newDates;
    }
}
