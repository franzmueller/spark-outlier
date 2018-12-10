package org.infai.senergy.benchmark.smartmeter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.util.Iterator;

public class Diff implements MapGroupsWithStateFunction<String,Double,Double,Double>{

    transient Logger logger;

    public Diff(){
        super();
        logger = LogManager.getLogger("Diff");
    }

    //Update function
    @Override
    public Double call(String key, Iterator<Double> values, GroupState<Double> state) throws Exception {
        Double oldValue = 0.0;
        if(state.exists())
            oldValue = state.get();
        while(values.hasNext()) {
            oldValue = values.next() - oldValue;
            logger.info("Set oldvalue as "+oldValue);
        }
        state.update(oldValue);
        return oldValue;
    }


}
