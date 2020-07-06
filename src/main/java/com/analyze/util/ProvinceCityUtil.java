package com.analyze.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.analyze.bean.LicenceProvince;
import org.apache.commons.collections.map.HashedMap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author: zhang yufei
 * @create: 2020-07-06 13:11
 **/
public class ProvinceCityUtil {

    public static Map<String,String> getLicenceProvince(String path) {
        Map<String,String> map=new HashedMap();
        String sql = "select license_initial,province_name from sys_province_city_info where city_level=1";
        try {
            DatabasePool dbp = DatabasePool.getInstance(path);
            DruidPooledConnection con = dbp.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            while (result.next()) {
                map.put(result.getString("license_initial"),result.getString("province_name"));
                System.out.println(result.getString("license_initial")+"\t"+result.getString("province_name"));
            }
            result.close();
            ps.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

}
