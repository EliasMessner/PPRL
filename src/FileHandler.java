import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.text.SimpleDateFormat;
import java.util.*;

public class FileHandler {

    /**
     * Parses the program parameters from a csv file. The first line of the file should specify the column names.
     * Each line contains one set of parameters for the main loop.
     * @param filePath path of the file
     * @return A list of the parameter objects represented by the file
     */
    public static List<Parameters> parseParametersListFromFile(String filePath) throws IOException {
        Map<String, Integer> attributeIndices;
        List<Parameters> parametersList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // handle first line separately
            String line = br.readLine();
            attributeIndices = getAttributeIndices(line);
            while ((line = br.readLine()) != null) {
                parametersList.add(parseParametersFromLine(line, attributeIndices));
            }
        }
        return parametersList;
    }

    /**
     * Writes the result record into a csv file in the "results" folder. Each record will be one line.
     * @param results list of result records.
     * @param dirPath csv file to write to.
     */
    public static void writeResults(List<Result> results, String dirPath, boolean timeStampInFileName) throws IOException {
        List<String> lines = new ArrayList<>(results.stream().map(Result::toCSVString).toList());
        lines.add(0, Result.getCSVHeadLine());
        Path dir = Paths.get(dirPath);
        String timeStamp = timeStampInFileName ? "_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) : "";
        Path file = Paths.get(dirPath, "results" + timeStamp + ".csv");
        if (!dir.toFile().exists()) {
            if (!dir.toFile().mkdir()) {
                throw new IOException("Unable to create results directory.");
            }
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
        System.out.printf("Output written to file '%s'\n", file);
    }

    /**
     * Reads file and returns array of Person objects.
     * @param filePath the filepath to the cvs file.
     * @param size the number of entries in the dataset.
     * @return array of Person instances represented by the file.
     */
    public static Person[] parseData(String filePath, int size) throws IOException {
        Person[] dataSet = new Person[size];
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        int i = 0;
        while ((line = br.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            String[] attributes = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // this regex means split by comma but only if there is an even number of quotation marks ahead
            dataSet[i] = new Person(attributes);
            i++;
        }
        return dataSet;
    }

    private static Parameters parseParametersFromLine(String line, Map<String, Integer> attributeIndices) {
        String[] args = line.trim().split(" *, *");
        return new Parameters(
                LinkingMode.parseFromString(args[attributeIndices.get("linkingMode")]),
                HashingMode.parseFromString(args[attributeIndices.get("hashingMode")]),
                Boolean.parseBoolean(args[attributeIndices.get("b")]),
                Boolean.parseBoolean(args[attributeIndices.get("wa")]),
                args[attributeIndices.get("sp")],
                Integer.parseInt(args[attributeIndices.get("l")]),
                Integer.parseInt(args[attributeIndices.get("k")]),
                Double.parseDouble(args[attributeIndices.get("t")]));
    }

    /**
     * Helper method for parseParametersListFromFile.
     */
    private static Map<String, Integer> getAttributeIndices(String line) {
        Map<String, Integer> attributeIndices = new HashMap<>();
        String[] args = line.trim().split(" *, *");
        for (int i = 0; i < args.length; i++) {
            attributeIndices.put(args[i], i);
        }
        return attributeIndices;
    }
}
