package store.ldb;

public class Utils {
    public static void requireTrue(boolean bool) {
        if (!bool) {
            throw new IllegalStateException("should be true");
        }
    }

    public static double roundTo(double score, int digits) {
        final double factor = Math.pow(10.0, digits);
        return ((long) (score * factor)) / factor;
    }

    public static void shouldNotGetHere(String s) {
        throw new IllegalStateException(s);
    }
}
