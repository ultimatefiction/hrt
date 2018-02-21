package com.veritas.hrt.model;

public class Permission {

    private String domain;
    private int level;

    public Permission(){}

    public Permission(String domain, int level) {
        this.domain = domain;
        this.level = level;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return String.format("{%s|%d}", domain, level);
    }
}
