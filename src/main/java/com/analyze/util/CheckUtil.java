package com.analyze.util;

import javafx.stage.StageStyle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: zhang yufei
 * @create: 2020-06-28 09:58
 **/
public class CheckUtil {

    //yyyy-MM-dd
    private static final String DATE_1="^(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29)$";
    //yyyy-MM-dd hh:mm:ss
    private static final String TIME_1="^(((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])$";

    public static Boolean checkDate_1(String str){
        Pattern p = Pattern.compile(DATE_1);
        Matcher m = p.matcher(str);
        return m.matches();
    }

    public static Boolean checkTime_1(String str){
        Pattern p = Pattern.compile(TIME_1);
        Matcher m = p.matcher(str);
        return m.matches();
    }

}
