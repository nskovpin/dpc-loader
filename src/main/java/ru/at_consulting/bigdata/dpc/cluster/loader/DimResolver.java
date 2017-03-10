package ru.at_consulting.bigdata.dpc.cluster.loader;

import ru.at_consulting.bigdata.dpc.json.DpcRoot;

/**
 * Created by NSkovpin on 27.02.2017.
 */
public interface DimResolver<T, O>{

    O resolve(T pojo, DpcRoot dpcRoot);

}
