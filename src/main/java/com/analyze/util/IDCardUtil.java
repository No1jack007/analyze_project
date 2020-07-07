package com.analyze.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.collections.map.HashedMap;

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
 * @create: 2020-07-07 11:23
 **/
public class IDCardUtil {

    public static Map<String, String> getProvince(String path) {
        Map<String, String> map = new HashedMap();
        String sql = "select code,array from sys_identity_array_code where type=1";
        try {
            DatabasePool dbp = DatabasePool.getInstance(path);
            DruidPooledConnection con = dbp.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            while (result.next()) {
                map.put(result.getString("code").substring(0, 2), result.getString("array"));
//                System.out.println(result.getString("license_initial")+"\t"+result.getString("province_name"));
            }
            result.close();
            ps.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static String parseGender(String cid) {
        String gender = null;
        char c = cid.charAt(cid.length() - 2);
        int sex = Integer.parseInt(String.valueOf(c));
        if (sex % 2 == 0) {
            gender = "女";
        } else {
            gender = "男";
        }
        return gender;
    }

    public static int parseAge(String cid) {
        int age = 0;
        String birthDayStr = cid.substring(6, 14);
        Date birthDay = null;
        try {
            birthDay = new SimpleDateFormat("yyyyMMdd").parse(birthDayStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        if (cal.before(birthDay)) {
            throw new IllegalArgumentException("您还没有出生么？");
        }
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH) + 1;
        int dayNow = cal.get(Calendar.DAY_OF_MONTH);
        cal.setTime(birthDay);
        int yearBirth = cal.get(Calendar.YEAR);
        int monthBirth = cal.get(Calendar.MONTH) + 1;
        int dayBirth = cal.get(Calendar.DAY_OF_MONTH);
        age = yearNow - yearBirth;
        if (monthNow <= monthBirth) {
            if (monthNow == monthBirth && dayNow < dayBirth) {
                age--;
            }
        } else {
            age--;
        }
        return age;
    }

    public static String parseAddress(String cid) {
        String address = null;
        String addressCode = cid.substring(0, 6);

        return address;
    }

}
