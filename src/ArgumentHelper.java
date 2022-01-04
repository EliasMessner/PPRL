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

    public static Parameters parseParametersFromArguments(String[] args) {
        double t = Double.parseDouble(ArgumentHelper.parseString(args, "t", null));
        int l = Integer.parseInt(ArgumentHelper.parseString(args, "l", null));
        int k = Integer.parseInt(ArgumentHelper.parseString(args, "k", "10"));
        HashingMode hashingMode = HashingMode.parseFromString(ArgumentHelper.parseString(args, "mode", "DH"));
        boolean blocking = ArgumentHelper.parseBoolean(args, "b", false) ||
                ArgumentHelper.parseBoolean(args, "blocking", false);
        boolean weightedAttributes = ArgumentHelper.parseBoolean(args, "wa", false) ||
                ArgumentHelper.parseBoolean(args, "weightedAttributes", false);
        return new Parameters(t, l, k, hashingMode, blocking, weightedAttributes);
    }

    public static String parseString(String[] args, String argName, String defaultValue) {
        String parsedValue = Arrays.stream(args).filter(a -> a.startsWith(argName + "="))
                .findFirst().orElse(defaultValue);
        return parsedValue.substring(parsedValue.indexOf("=")+1);
    }

}
