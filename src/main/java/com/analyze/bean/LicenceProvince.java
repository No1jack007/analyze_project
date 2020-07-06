package com.analyze.bean;

/**
 * @author: zhang yufei
 * @create: 2020-07-06 13:24
 **/
public class LicenceProvince {

    private String licenseInitial;

    private String provinceName;

    public LicenceProvince(String licenseInitial, String provinceName) {
        this.licenseInitial = licenseInitial;
        this.provinceName = provinceName;
    }

    public String getLicenseInitial() {
        return licenseInitial;
    }

    public void setLicenseInitial(String licenseInitial) {
        this.licenseInitial = licenseInitial;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }
}
