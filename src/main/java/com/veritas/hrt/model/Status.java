package com.veritas.hrt.model;

public enum Status {

    ACTIVE(1), RESTRICTED(2), SUSPENDED(3);

    private int value;

    private static final String ACTIVE_STRING = "active";
    private static final String RESTRICTED_STRING = "restricted";
    private static final String SUSPENDED_STRING = "suspended";

    Status(int value) {
        this.value = value;
    }

    public static Status forString(String string) {
        string = string.toLowerCase();
        switch (string) {
            case ACTIVE_STRING:
                return ACTIVE;
            case RESTRICTED_STRING:
                return RESTRICTED;
            case SUSPENDED_STRING:
                return SUSPENDED;
        }
        return ACTIVE;
    }

    public static Status forValue(int value) {
        switch (value) {
            case 1:
                return ACTIVE;
            case 2:
                return RESTRICTED;
            case 3:
                return SUSPENDED;
        }
        return ACTIVE;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        switch (this) {
            case ACTIVE:
                return ACTIVE_STRING;
            case RESTRICTED:
                return RESTRICTED_STRING;
            case SUSPENDED:
                return SUSPENDED_STRING;
        }
        return ACTIVE_STRING;
    }

}
