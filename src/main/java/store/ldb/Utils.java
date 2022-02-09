
public class Utils {
    public static void requireTrue(boolean bool) {
        if (!bool) {
            throw new IllegalStateException("should be true");
        }
    }
}
