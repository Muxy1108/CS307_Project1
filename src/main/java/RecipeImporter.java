import com.opencsv.*;
import java.io.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RecipeImporter {
    private static final int EXPECTED_COLUMNS = 27;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;

    public static void main(String[] args) {
        String csvPath = "data/recipes.csv";
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres",
                "postgres", "Xieyan2005")) {

            conn.setAutoCommit(false);
            importRecipesParallel(conn, csvPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** ---------------- PARALLEL IMPORT ---------------- */
    private static void importRecipesParallel(Connection conn, String csvPath) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        List<String[]> rows = new ArrayList<>();
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvPath))
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(',')
                        .withQuoteChar('"')
                        .withStrictQuotes(false)
                        .build())
                .build()) {

            String[] header = reader.readNext();
            if (header == null) {
                System.err.println("❌ Empty CSV file.");
                return;
            }

            System.out.println("Header: " + String.join(",", header));

            String[] line;
            while (true) {
                try {
                    line = reader.readNext();
                    if (line == null) break;
                    if (line.length < EXPECTED_COLUMNS) continue;
                    rows.add(line);
                } catch (com.opencsv.exceptions.CsvMalformedLineException ex) {
                    // skip malformed line, but continue import
                    System.err.println("⚠️ Skipping malformed line: " + ex.getMessage());
                }
            }
        }

        if (rows.isEmpty()) {
            System.err.println("❌ No valid rows found.");
            return;
        }

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());
        PrintWriter errorLog = new PrintWriter(new FileWriter("import_errors.log", true));
        int[] inserted = {0};
        int[] skipped = {0};

        for (List<String[]> batch : partitions) {
            pool.submit(() -> {
                try (PreparedStatement ps = conn.prepareStatement(getInsertSQL())) {
                    for (String[] cols : batch) {
                        try {
                            List<String> c = padToExpected(cols);
                            fillPreparedStatement(ps, c);
                            ps.addBatch();
                            inserted[0]++;
                        } catch (Exception ex) {
                            synchronized (errorLog) {
                                errorLog.println("⚠️ Parse error: " + Arrays.toString(cols));
                                ex.printStackTrace(errorLog);
                            }
                            skipped[0]++;
                        }
                    }
                    synchronized (conn) {
                        ps.executeBatch();
                        conn.commit();
                    }
                } catch (Exception e) {
                    synchronized (errorLog) { e.printStackTrace(errorLog); }
                } finally { latch.countDown(); }
            });
        }

        latch.await();
        pool.shutdown();
        errorLog.close();

        System.out.println("✅ Done. inserted=" + inserted[0] + ", skippedParse=" + skipped[0]);
        System.out.println("✅ Import finished.");
    }

    /** ---------------- SQL + MAPPERS ---------------- */
    private static String getInsertSQL() {
        return """
            INSERT INTO recipes (
                recipe_id, recipe_name, author_id, author_name, cook_time, prep_time,
                total_time, date_published, description, recipe_category, keywords,
                recipe_ingredient, aggregated_rating, review_count, calories, fat_content,
                saturated_fat_content, cholesterol_content, sodium_content, carbohydrate_content,
                fiber_content, sugar_content, protein_content, recipe_servings, recipe_yield,
                recipe_instructions, favorite_users
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (recipe_id) DO NOTHING
        """;
    }

    private static void fillPreparedStatement(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setInt(1, safeInt(c.get(0)));
        ps.setString(2, safeStr(c.get(1)));
        ps.setInt(3, safeInt(c.get(2)));
        ps.setString(4, safeStr(c.get(3)));
        ps.setString(5, safeStr(c.get(4)));
        ps.setString(6, safeStr(c.get(5)));
        ps.setString(7, safeStr(c.get(6)));
        ps.setObject(8, parseTimestamp(c.get(7)));
        ps.setString(9, safeStr(c.get(8)));
        ps.setString(10, safeStr(c.get(9)));
        ps.setString(11, normalizeArray(c.get(10)));
        ps.setString(12, normalizeArray(c.get(11)));
        ps.setObject(13, safeDouble(c.get(12)));
        ps.setObject(14, safeDouble(c.get(13)));
        ps.setObject(15, safeDouble(c.get(14)));
        ps.setObject(16, safeDouble(c.get(15)));
        ps.setObject(17, safeDouble(c.get(16)));
        ps.setObject(18, safeDouble(c.get(17)));
        ps.setObject(19, safeDouble(c.get(18)));
        ps.setObject(20, safeDouble(c.get(19)));
        ps.setObject(21, safeDouble(c.get(20)));
        ps.setObject(22, safeDouble(c.get(21)));
        ps.setObject(23, safeDouble(c.get(22)));
        ps.setObject(24, safeDouble(c.get(23)));
        ps.setString(25, safeStr(c.get(24)));
        ps.setString(26, safeStr(c.get(25)));
        ps.setString(27, normalizeArray(c.get(26)));
    }

    /** ---------------- HELPERS ---------------- */
    private static List<String> padToExpected(String[] cols) {
        List<String> c = new ArrayList<>(Arrays.asList(cols));
        while (c.size() < EXPECTED_COLUMNS) c.add(null);
        if (c.size() > EXPECTED_COLUMNS) c = c.subList(0, EXPECTED_COLUMNS);
        return c;
    }

    private static String safeStr(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;
        return s.length() > 200 ? s.substring(0, 200) : s;
    }

    private static Integer safeInt(String s) {
        try { return (s == null || s.trim().isEmpty()) ? null : Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }

    private static Double safeDouble(String s) {
        try { return (s == null || s.trim().isEmpty()) ? null : Double.parseDouble(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }

    private static Timestamp parseTimestamp(String s) {
        try { return (s == null || s.isEmpty()) ? null :
                Timestamp.from(Instant.parse(s.replace("Z", "+00:00"))); }
        catch (Exception e) { return null; }
    }

    private static String normalizeArray(String s) {
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
}
