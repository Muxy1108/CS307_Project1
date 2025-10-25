import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Safety {

    private static final int EXPECTED_COLUMNS = 27;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";


    public static String safeStr(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;
        return s.length() > 200 ? s.substring(0, 200) : s;
    }

    public static Integer safeInt(String s) {
        try { return (s == null || s.trim().isEmpty()) ? null : Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }

    public static Double safeDouble(String s) {
        try { return (s == null || s.trim().isEmpty()) ? null : Double.parseDouble(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }

    public static Timestamp parseTimestamp(String s) {
        try { return (s == null || s.isEmpty()) ? null :
                Timestamp.from(Instant.parse(s.replace("Z", "+00:00"))); }
        catch (Exception e) { return null; }
    }

    public static String normalizeArray(String s) {
        if (s == null) return "[]";
        s = s.replaceAll("^c\\(|\\)$", "")
                .replaceAll("\"\"", "\"")
                .replaceAll("\"", "")
                .replaceAll("\\s+", " ")
                .trim();
        if (s.isEmpty()) return "[]";
        String[] parts = s.split(",");
        return Arrays.stream(parts)
                .map(String::trim)
                .filter(p -> !p.isEmpty())
                .map(p -> "\"" + p + "\"")
                .collect(Collectors.joining(",", "[", "]"));
    }

    public static List<String> padToExpected(String[] cols) {
        List<String> c = new ArrayList<>(Arrays.asList(cols));
        while (c.size() < EXPECTED_COLUMNS) c.add(null);
        if (c.size() > EXPECTED_COLUMNS) c = c.subList(0, EXPECTED_COLUMNS);
        return c;
    }

}
