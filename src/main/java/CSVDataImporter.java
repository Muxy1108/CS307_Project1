import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;
import org.json.JSONArray;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

public class CSVDataImporter {
    private static final String CSV_PATH = "data/recipes.csv";
    private static final String JDBC_URL  = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";
    private static final int BATCH_SIZE = 1000;

    private static final String INSERT_SQL = """
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

    public static void main(String[] args) {
        System.out.println("Starting import...");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {
            conn.setAutoCommit(false);
            importRecipes(conn, CSV_PATH);
            System.out.println("✅ Import finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void importRecipes(Connection conn, String csvPath) throws IOException, SQLException {
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .withEscapeChar('"') // handle inner double quotes properly
                .withIgnoreQuotations(false)
                .build();

        try (CSVReader reader = new CSVReaderBuilder(
                new BufferedReader(
                        new InputStreamReader(new FileInputStream(csvPath), StandardCharsets.UTF_8))
        ).withCSVParser(parser)
                .withSkipLines(1) // skip header
                .build();
             PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL);
             BufferedWriter errorLog = Files.newBufferedWriter(
                     Paths.get("import_errors.log"),
                     StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.TRUNCATE_EXISTING)
        ) {

            String[] fields;
            int inserted = 0;
            int lineNum = 1;
            List<String[]> batch = new ArrayList<>(BATCH_SIZE);

            while ((fields = reader.readNext()) != null) {
                lineNum++;

                if (fields.length < 27) {
                    fields = padToLength(fields, 27);
                }

                batch.add(fields);
                if (batch.size() >= BATCH_SIZE) {
                    inserted += executeBatchInsert(conn, pstmt, batch, errorLog, lineNum - batch.size());
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                inserted += executeBatchInsert(conn, pstmt, batch, errorLog, lineNum - batch.size());
            }

            conn.commit();
            System.out.printf("✅ Done. inserted=%d (see import_errors.log for skipped rows)%n", inserted);
        } catch (CsvValidationException e) {
            throw new IOException("CSV parse error", e);
        }
    }

    // --- Utilities ---
    private static String[] padToLength(String[] fields, int expectedLength) {
        String[] padded = new String[expectedLength];
        Arrays.fill(padded, null);
        System.arraycopy(fields, 0, padded, 0, Math.min(fields.length, expectedLength));
        return padded;
    }

    private static int executeBatchInsert(Connection conn, PreparedStatement pstmt, List<String[]> records,
                                          BufferedWriter errorLog, int startLineHint) throws SQLException, IOException {
        int inserted = 0;
        pstmt.clearBatch();
        int idx = 0;
        for (String[] f : records) {
            idx++;
            try {
                bindFieldsToStatement(pstmt, f);
                pstmt.addBatch();
            } catch (Exception e) {
                errorLog.write("⚠️ Bind error at line " + (startLineHint + idx) + ": " + e.getMessage() + "\n");
            }
        }

        try {
            int[] results = pstmt.executeBatch();
            for (int r : results) if (r >= 0 || r == Statement.SUCCESS_NO_INFO) inserted++;
            conn.commit();
        } catch (BatchUpdateException bue) {
            errorLog.write("⚠️ Batch exception: " + bue.getMessage() + "\n");
            conn.rollback();
        }
        return inserted;
    }

    // --- Field binding ---
    private static void bindFieldsToStatement(PreparedStatement pstmt, String[] f) throws Exception {
        int param = 1;
        pstmt.setInt(param++, safeParseInt(f, 0));
        pstmt.setString(param++, safeOrNull(f, 1));
        pstmt.setInt(param++, safeParseInt(f, 2));
        pstmt.setString(param++, safeOrNull(f, 3));
        pstmt.setString(param++, safeOrNull(f, 4));
        pstmt.setString(param++, safeOrNull(f, 5));
        pstmt.setString(param++, safeOrNull(f, 6));
        pstmt.setTimestamp(param++, parseTimestampOrNull(safeOrNull(f, 7)));
        pstmt.setString(param++, safeOrNull(f, 8));
        pstmt.setString(param++, safeOrNull(f, 9));
        pstmt.setString(param++, normalizeToJsonArray(safeOrNull(f, 10)));
        pstmt.setString(param++, normalizeToJsonArray(safeOrNull(f, 11)));
        pstmt.setObject(param++, safeParseDoubleObj(f, 12), Types.DOUBLE);
        pstmt.setObject(param++, safeParseDoubleObj(f, 13), Types.DOUBLE);
        for (int i = 14; i <= 22; i++) pstmt.setObject(param++, safeParseDoubleObj(f, i), Types.DOUBLE);
        pstmt.setObject(param++, safeParseDoubleObj(f, 23), Types.DOUBLE);
        pstmt.setString(param++, safeOrNull(f, 24));
        pstmt.setString(param++, normalizeToJsonArray(safeOrNull(f, 25)));
        pstmt.setString(param++, normalizeFavoriteUsersToJson(safeOrNull(f, 26)));
    }

    // --- Helpers ---
    private static int safeParseInt(String[] f, int idx) {
        try { return (idx >= f.length || f[idx] == null || f[idx].isEmpty()) ? 0 : Integer.parseInt(f[idx].trim()); }
        catch (Exception e) { return 0; }
    }

    private static Double safeParseDoubleObj(String[] f, int idx) {
        try { return (idx >= f.length || f[idx] == null || f[idx].isEmpty()) ? null : Double.parseDouble(f[idx].trim()); }
        catch (Exception e) { return null; }
    }

    private static String safeOrNull(String[] f, int idx) {
        if (idx >= f.length) return null;
        String s = f[idx];
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty()) return null;
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() > 1) s = s.substring(1, s.length() - 1);
        return s;
    }

    private static Timestamp parseTimestampOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Timestamp.from(Instant.parse(s)); }
        catch (DateTimeParseException e) {
            try {
                String cleaned = s.replace('T', ' ');
                return Timestamp.valueOf(cleaned);
            } catch (Exception ex) { return null; }
        }
    }

    private static String normalizeToJsonArray(String raw) {
        if (raw == null || raw.isEmpty()) return "[]";
        raw = raw.trim();
        try {
            if (raw.startsWith("c(") && raw.endsWith(")")) {
                List<String> items = parseQuotedItems(raw.substring(2, raw.length() - 1));
                JSONArray arr = new JSONArray();
                for (String s : items) arr.put(s);
                return arr.toString();
            }
            return new JSONArray().put(raw).toString();
        } catch (Exception e) {
            return "[]";
        }
    }

    private static String normalizeFavoriteUsersToJson(String raw) {
        if (raw == null || raw.isEmpty()) return "[]";
        raw = raw.replaceAll("^\"+|\"+$", "");
        JSONArray arr = new JSONArray();
        for (String s : raw.split(",")) {
            s = s.trim();
            if (s.isEmpty()) continue;
            try { arr.put(Long.parseLong(s)); }
            catch (Exception e) { arr.put(s); }
        }
        return arr.toString();
    }

    private static List<String> parseQuotedItems(String inner) {
        List<String> out = new ArrayList<>();
        if (inner == null || inner.isEmpty()) return out;
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '"') inQuote = !inQuote;
            else if (c == ',' && !inQuote) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else cur.append(c);
        }
        if (!cur.isEmpty()) out.add(cur.toString().trim());
        return out;
    }
}
