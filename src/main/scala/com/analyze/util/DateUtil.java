package com.analyze.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: zhang yufei
 * @create: 2020-06-10 17:55
 **/
public class DateUtil {

    public static int getDutyDays(String start,String end){

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate=null;
        Date endDate = null;
        try {
            startDate=df.parse(start);
            endDate = df.parse(end);
        } catch (ParseException e) {
            System.out.println("非法的日期格式,无法进行转换");
            e.printStackTrace();
        }
        int result = 0;
        while (startDate.compareTo(endDate) <= 0) {
            if (startDate.getDay() != 6 && startDate.getDay() != 0)
                result++;
            startDate.setDate(startDate.getDate() + 1);
        }
        return result;
    }


}
