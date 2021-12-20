import java.util.Arrays;

public class ArgumentHelper {

    /**
     * Parse boolean value from argument list.
     * @param args argument list
     * @param argName name of the argument
     * @return parsed value or default value if not found
     */
    public static boolean parseBoolean(String[] args, String argName, boolean defaultValue) {
        String parsedValue = Arrays.stream(args).filter(a -> a.startsWith(argName + "="))
                .findFirst().orElse(Boolean.toString(defaultValue));
        if (parsedValue.endsWith("true")) {
            return true;
        } else if (parsedValue.endsWith("false")) {
            return false;
        } else {
            throw new IllegalArgumentException("Cannot read boolean argument value '" + parsedValue + "'");
        }
    }

    public static String parseString(String[] args, String argName, String defaultValue) {
        String parsedValue = Arrays.stream(args).filter(a -> a.startsWith(argName + "="))
                .findFirst().orElse(defaultValue);
        return parsedValue.substring(parsedValue.indexOf("=")+1);
    }

}
