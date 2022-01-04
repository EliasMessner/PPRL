import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileHandler {

    /**
     * Parses the program parameters from a csv file. The first line of the file should specify the column names.
     * Each line contains one set of parameters for the main loop.
     * @param filePath path of the file
     * @return A list of the parameter objectes represented by the file
     */
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

    /**
     * Writes the result record into a csv file in the "results" folder. Each record will be one line.
     * @param results list of result records.
     * @param filePath csv file to write to.
     * @throws IOException
     */
    public static void writeResults(List<Result> results, String filePath) throws IOException {
        List<String> lines = new ArrayList<>(results.stream().map(Result::toCSVString).toList());
        lines.add(0, Result.getCSVHeadLine());
        Path file = Paths.get(filePath);
        if (!file.getParent().toFile().exists()) {
            if (!file.getParent().toFile().mkdir()) {
                throw new IOException("Unable to create results directory.");
            }
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
    }

    private static Parameters parseParametersFromLine(String line, Map<String, Integer> attributeIndices) {
        String[] args = line.trim().split(" *, *");
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
        String[] args = line.trim().split(" *, *");
        assert args.length == 6;
        for (int i = 0; i < args.length; i++) {
            attributeIndices.put(args[i], i);
        }
        return attributeIndices;
    }
}
