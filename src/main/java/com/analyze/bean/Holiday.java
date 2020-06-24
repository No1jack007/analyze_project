package com.analyze.bean;

import java.io.Serializable;

/**
 * @author: zhang yufei
 * @create: 2020-06-18 09:42
 **/
public class Holiday implements Serializable {

    private String holidays;

    private String workdays;

    public Holiday(String holidays,String workdays){
        this.holidays=holidays;
        this.workdays=workdays;
    }

    public String getHolidays() {
        return holidays;
    }

    public void setHolidays(String holidays) {
        this.holidays = holidays;
    }

    public String getWorkdays() {
        return workdays;
    }

    public void setWorkdays(String workdays) {
        this.workdays = workdays;
    }
}
