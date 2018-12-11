package org.infai.senergy.benchmark.smartmeter.util;

import java.sql.Timestamp;

public class DateValuePair<T> {
    T value;
    Timestamp time;
    public DateValuePair(Timestamp time, T value){
        this.value = value;
        this.time = time;
    }

    public Timestamp getTime(){
        return time;
    }

    public T getValue(){
        return value;
    }
}
