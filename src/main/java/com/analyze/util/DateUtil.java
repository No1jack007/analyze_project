package com.analyze.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.pool.vendor.SybaseExceptionSorter;
import com.analyze.bean.Holiday;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * @author: zhang yufei
 * @create: 2020-06-10 17:55
 **/
public class DateUtil implements Serializable {


    public static Holiday getHoliday(String path) {
        String sql = "select * from sys_holiday";
        try {
            DatabasePool dbp = DatabasePool.getInstance(path);
            DruidPooledConnection con = dbp.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            StringBuffer holidays = new StringBuffer();
            StringBuffer workdays = new StringBuffer();
            while (result.next()) {
                if ("1".equals(result.getString("type"))) {
                    holidays.append(result.getString("holiday")).append(",");
                } else if ("2".equals(result.getString("type"))) {
                    workdays.append(result.getString("holiday")).append(",");
                }
            }
            result.close();
            ps.close();
            con.close();
            System.out.println("holidays" + "\t" + holidays);
            System.out.println("workdays" + "\t" + workdays);
            return new Holiday(holidays.toString(), workdays.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static int countWorkDay(String start, String end, Holiday holiday) {
//        System.out.println("start:\t"+start);
//        System.out.println("end:\t"+end);
        Date startDate = null;
        Date endDate = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            startDate = sdf.parse(start);
            endDate = sdf.parse(end);
        } catch (ParseException e) {
            System.out.println("非法的日期格式,无法进行转换");
            e.printStackTrace();
            return -1;
        }
        int result = 0;
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(startDate);
        while (startCalendar.getTime().getTime() < endDate.getTime()) {
            if (startCalendar.get(Calendar.DAY_OF_WEEK) != 1 && startCalendar.get(Calendar.DAY_OF_WEEK) != 7) {
                result++;
                if (holiday.getHolidays().indexOf(sdf.format(startCalendar.getTime())) > -1) {
                    result--;
                }
            } else {
                if (holiday.getWorkdays().indexOf(sdf.format(startCalendar.getTime())) > -1) {
                    result++;
                }
            }
            startCalendar.add(startCalendar.DATE, 1);
        }
        return result;
    }

    public static int getDutyDays(Date StartDate, Date EndDate, Holiday holiday) {//得到非周六周日的工作日
        StartDate.setDate(StartDate.getDate() + 1);
        int result = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        while (StartDate.compareTo(EndDate) <= 0) {
            if (StartDate.getDay() != 6 && StartDate.getDay() != 0) {
                //周内
                result++;
                // System.out.println(holidays);
                if (holiday.getHolidays().length() > 0) {
                    if (holiday.getHolidays().indexOf(sdf.format(StartDate)) > -1)//不是节假日加1
                        result--;
                }
            } else {
                //周末
                if (holiday.getHolidays().length() > 0) {
                    if (holiday.getHolidays().indexOf(sdf.format(StartDate)) > -1)//工作日
                        result++;
                }
            }
            StartDate.setDate(StartDate.getDate() + 1);
        }
        return result;
    }

}
