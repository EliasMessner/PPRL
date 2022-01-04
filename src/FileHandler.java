import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class FileHandler {

    public static List<Parameters> parseParametersListFromFile(String filePath) throws IOException {
        List<Parameters> parametersList = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(line -> parametersList.add(parseParametersFromLine(line)));
        }
        return parametersList;
    }

    private static Parameters parseParametersFromLine(String line) {
        String[] args = line.trim().split(" ");
        assert args.length == 6;
        return new Parameters(
                Double.parseDouble(args[0]),
                Integer.parseInt(args[1]),
                Integer.parseInt(args[2]),
                HashingMode.parseFromString(args[3]),
                Boolean.parseBoolean(args[4]),
                Boolean.parseBoolean(args[5]));
    }

}
