import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.opencsv.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Safety {
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
        if (s == null || s.trim().isEmpty()) return null;
        try {
            String trimmed = s.trim();
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException e1) {
            try {
                String trimmed = s.trim();
                if (trimmed.contains(".")) {
                    double doubleValue = Double.parseDouble(trimmed);
                        return (int) doubleValue;
                }
                return null;
            } catch (NumberFormatException e2){
                return null;
            }
        }
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

    public static List<String> padToExpected(String[] cols,int EXPECTED_COLUMNS) {
        List<String> c = new ArrayList<>(Arrays.asList(cols));
        while (c.size() < EXPECTED_COLUMNS) c.add(null);
        if (c.size() > EXPECTED_COLUMNS) c = c.subList(0, EXPECTED_COLUMNS);
        return c;
    }

    public static List<Integer> parseIds(String Data) {
        List<Integer> Ids = new ArrayList<>();
        if (Data == null || Data.trim().isEmpty()) {
            return Ids;
        }

        try {
            String cleaned = Data.trim();
            if(cleaned.isEmpty() || cleaned.equals("null") || cleaned.equals("NULL")){
                return Ids;
            }

            String[] parts = cleaned.split(",");
            for (String part : parts) {
                String trimmed = part.trim();
                Integer Id = Safety.safeInt(trimmed);
                if (Id != null) {
                    Ids.add(Id);
                }
            }
        } catch (Exception e) {
        }
        return Ids;
    }

    public static List<String> parseStrs(String data) {
        List<String> result = new ArrayList<>();

        // 空值检查
        if (data == null || data.trim().isEmpty() ||
                data.trim().equals("null") || data.trim().equals("NULL")) {
            return result;
        }

        String cleaned = data.trim();
        if (cleaned.isEmpty()) {
            return result;
        }

        List<String> parsedItems = parseWithQuotes(cleaned);

        for (String item : parsedItems) {
            String safeStr = Safety.safeStr(item.trim());
            if (safeStr.startsWith("c(") && safeStr.endsWith(")")) {
                String content = safeStr.substring(2, safeStr.length() - 1);
                result.add(content);
            }
            else if (safeStr.startsWith("[") && safeStr.endsWith("]")) {
                String content = safeStr.substring(1, safeStr.length() - 1);
                result.add(content);
            }

            else if (!safeStr.isEmpty()) {
                result.add(safeStr);
            }
        }

        return result;
    }


    private static List<String> parseWithQuotes(String content) {
        List<String> items = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if ((c == '"' || c == '\'') && (i == 0 || content.charAt(i-1) != '\\')) {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuotes = false;
                }
            } else if (c == ',' && !inQuotes) {
                String item = current.toString().trim();
                if (!item.isEmpty()) {
                    items.add(item);
                }
                current.setLength(0);
                continue;
            }

            current.append(c);
        }

        String lastItem = current.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }

        return items;
    }

    public static void deleteAll() throws SQLException{
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()){

            String sql ="drop table if exists users CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists user_followers CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists user_following CASCADE";
            stmt.execute(sql);

            sql ="drop table if exists recipes CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists keywords CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists recipe_instructions CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists recipe_ingredients CASCADE";
            stmt.execute(sql);

            sql ="drop table if exists reviews CASCADE";
            stmt.execute(sql);
            sql ="drop table if exists review_likers CASCADE";
            stmt.execute(sql);

            System.out.println("所有表删除完毕");
        }


    }

}
