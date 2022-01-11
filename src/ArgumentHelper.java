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
        if (parsedValue.toLowerCase().endsWith("true")) {
            return true;
        } else if (parsedValue.toLowerCase().endsWith("false")) {
            return false;
        } else {
            throw new IllegalArgumentException("Cannot read boolean argument value '" + parsedValue + "'");
        }
    }

    public static Parameters parseParametersFromArguments(String[] args) {
        HashingMode hashingMode = HashingMode.parseFromString(ArgumentHelper.parseString(args, "mode", "DH"));
        boolean blocking = ArgumentHelper.parseBoolean(args, "b", true) ||
                ArgumentHelper.parseBoolean(args, "blocking", true);
        boolean weightedAttributes = ArgumentHelper.parseBoolean(args, "wa", true) ||
                ArgumentHelper.parseBoolean(args, "weightedAttributes", true);
        String tokenSalting = ArgumentHelper.parseString(args, "ts", "");
        int l = Integer.parseInt(ArgumentHelper.parseString(args, "l", null));
        int k = Integer.parseInt(ArgumentHelper.parseString(args, "k", "10"));
        double t = Double.parseDouble(ArgumentHelper.parseString(args, "t", null));
        return new Parameters(hashingMode, blocking, weightedAttributes, tokenSalting, l, k, t);
    }

    public static String parseString(String[] args, String argName, String defaultValue) {
        String parsedValue = Arrays.stream(args).filter(a -> a.startsWith(argName + "="))
                .findFirst().orElse(defaultValue);
        return parsedValue.substring(parsedValue.indexOf("=")+1);
    }

}
