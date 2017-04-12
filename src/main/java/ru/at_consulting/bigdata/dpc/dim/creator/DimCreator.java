package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.DimEntity;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;

import java.io.Serializable;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public interface DimCreator<T extends DimEntity, PARENT> extends Serializable{

    <HOLDER extends IterableChildren> List<T> create(DpcRoot dpcRoot, HOLDER holder);

    T create(DpcRoot dpcRoot, PARENT parent);
}
