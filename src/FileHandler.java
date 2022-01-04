import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class FileHandler {

    public static List<Parameters> parseParametersListFromFile(String filePath) throws IOException {
        Map<String, Integer> attributeIndices;
        List<Parameters> parametersList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            attributeIndices = handleFirstLine(line);
            while ((line = br.readLine()) != null) {
                parametersList.add(parseParametersFromLine(line, attributeIndices));
            }
        }
        return parametersList;
    }

    private static Parameters parseParametersFromLine(String line, Map<String, Integer> attributeIndices) {
        String[] args = line.trim().split("[, ]");
        assert args.length == 6;
        return new Parameters(
                HashingMode.parseFromString(args[attributeIndices.get("mode")]),
                Boolean.parseBoolean(args[attributeIndices.get("b")]),
                Boolean.parseBoolean(args[attributeIndices.get("wa")]),
                Integer.parseInt(args[attributeIndices.get("l")]),
                Integer.parseInt(args[attributeIndices.get("k")]),
                Double.parseDouble(args[attributeIndices.get("t")]));
    }

    private static Map<String, Integer> handleFirstLine(String line) {
        Map<String, Integer> attributeIndices = new HashMap<>();
        String[] args = line.trim().split("[, ]");
        assert args.length == 6;
        for (int i = 0; i < args.length; i++) {
            attributeIndices.put(args[i], i);
        }
        return attributeIndices;
    }

}
