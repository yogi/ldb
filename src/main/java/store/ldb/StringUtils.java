package store.ldb;

public class StringUtils {
    static boolean isWithinRange(String s, String from, String to) {
        return isGreaterThanOrEqual(s, from) && isLessThanOrEqual(s, to);
    }

    static boolean isGreaterThanOrEqual(String s, String other) {
        return s.compareTo(other) >= 0;
    }

    static boolean isLessThanOrEqual(String s, String other) {
        return s.compareTo(other) <= 0;
    }
}
