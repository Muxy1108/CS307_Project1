import com.opencsv.*;
import java.io.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class RecipeKeywordsImporter {

    private static final int EXPECTED_COLUMNS = 27;
    private static final int BATCH_SIZE = 1000;
    private static final int THREAD_COUNT = 6;
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String JDBC_USER = "postgres";
    private static final String JDBC_PASS = "Xieyan2005";


    public static void importRecipesAndKeywords(String csvPath) throws Exception {
        System.out.println("开始导入 recipes 和 keywords 数据...");

        // 先检查原始数据格式
        checkRawKeywordsFromCSV(csvPath);

        List<String[]> rows = readCSVWithErrorHandling(csvPath);

        if (rows.isEmpty()) {
            System.err.println("No valid rows found.");
            return;
        }

        // 创建 keywords 表
        createKeywordsTable();

        // 导入数据 - 无批处理版本
        importDataWithKeywordsNoBatch(rows);
    }

    public static List<String[]> readCSVWithErrorHandling(String csvPath) {
        List<String[]> rows = new ArrayList<>();

        System.out.println("开始读取 CSV 文件...");

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .withEscapeChar('\\')
                .withIgnoreQuotations(false)
                .withStrictQuotes(false)
                .build();

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath));
             CSVReader reader = new CSVReaderBuilder(br)
                     .withCSVParser(parser)
                     .withSkipLines(0)
                     .build()) {

            // 读取头部
            String[] header = reader.readNext();
            if (header == null) {
                System.err.println("Empty CSV file.");
                return rows;
            }
            System.out.println("CSV头部 (" + header.length + "列): " + String.join("|", header));

            String[] line;
            int lineCount = 0;
            int errorCount = 0;

            while (true) {
                try {
                    line = reader.readNext();
                    if (line == null) break;

                    lineCount++;

                    if (line.length >= EXPECTED_COLUMNS) {
                        rows.add(line);
                    } else {
                        //System.err.println("跳过列数不足的行 " + lineCount + ": " + line.length + " 列");
                        errorCount++;
                    }

                    if (lineCount % 10000 == 0) {
                        System.out.println("已读取行数: " + lineCount + ", 错误: " + errorCount);
                    }

                } catch (Exception e) {
                    errorCount++;
                    //System.err.println("读取行 " + lineCount + " 时出错: " + e.getMessage());

                    /*if (errorCount > 100) {
                        System.err.println("错误过多，停止读取");
                        break;
                    }*/
                }
            }

            System.out.println("读取完成: 总行数 = " + lineCount + ", 有效行 = " + rows.size() + ", 错误 = " + errorCount);

        } catch (Exception e) {
            System.err.println("读取 CSV 文件失败: " + e.getMessage());
            e.printStackTrace();
        }

        return rows;
    }


    public static void debugKeywordsData(List<String[]> rows) {
        //System.out.println("🔍 examine keywords data...");

        int totalRows = rows.size();
        int nonEmptyKeywords = 0;
        int emptyKeywords = 0;
        int nullKeywords = 0;

        for (int i = 0; i < totalRows; i++) {
            String[] cols = rows.get(i);
            if (cols.length > 10) {
                String rawKeywords = cols[10];
                if (rawKeywords == null) {
                    nullKeywords++;
                } else if (rawKeywords.trim().isEmpty()) {
                    emptyKeywords++;
                } else {
                    nonEmptyKeywords++;
                }
            }
        }

        System.out.println("Keywords 统计:");
        System.out.println("总行数: " + totalRows);
        System.out.println("非空 keywords: " + nonEmptyKeywords);
        System.out.println("空 keywords(空格而非null): " + emptyKeywords);
        System.out.println("null keywords: " + nullKeywords);
    }

    public static void importDataWithKeywordsDebug(List<String[]> rows) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger recipesInserted = new AtomicInteger(0);
        AtomicInteger keywordsInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger keywordsProcessed = new AtomicInteger(0);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
            partitions.add(rows.subList(i, Math.min(i + BATCH_SIZE, rows.size())));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入，分区数: " + partitions.size());

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchRecipes = 0;
                int batchKeywords = 0;
                int batchSkipped = 0;
                int batchKeywordsProcessed = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement recipeStmt = conn.prepareStatement(getInsertSQLWithoutKeywords());
                     PreparedStatement keywordStmt = conn.prepareStatement(
                             "INSERT INTO keywords (recipe_id, keyword_text) VALUES (?, ?)")) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols);
                            Integer recipeId = Safety.safeInt(c.get(0));

                            if (recipeId == null) {
                                batchSkipped++;
                                continue;
                            }


                            fillPreparedStatementWithoutKeywords(recipeStmt, c);
                            int recipeResult = recipeStmt.executeUpdate();

                            if (recipeResult > 0) {
                                batchRecipes++;

                                String rawKeywords = c.get(10);
                                if (rawKeywords != null && !rawKeywords.trim().isEmpty()) {
                                    batchKeywordsProcessed++;

                                    List<String> keywords = parseCSVKeywords(rawKeywords);

                                    if (!keywords.isEmpty()) {
                                        System.out.println("分区 " + partitionIndex + " - Recipe " + recipeId +
                                                " 找到 " + keywords.size() + " 个 keywords: " + keywords);

                                        for (String keyword : keywords) {
                                            if (keyword.length() > 255) {
                                                keyword = keyword.substring(0, 255);
                                            }

                                            keywordStmt.setInt(1, recipeId);
                                            keywordStmt.setString(2, keyword);
                                            int keywordResult = keywordStmt.executeUpdate();

                                            if (keywordResult > 0) {
                                                batchKeywords++;
                                            }
                                        }
                                    } else {
                                        System.out.println("分区 " + partitionIndex + " - Recipe " + recipeId +
                                                " 解析 keywords 但结果为空: '" + rawKeywords + "'");
                                    }
                                }
                            }
                        } catch (Exception ex) {
                            batchSkipped++;
                            //System.err.println("分区 " + partitionIndex + " 插入失败: " + ex.getMessage());
                        }
                    }

                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    recipesInserted.addAndGet(batchRecipes);
                    keywordsInserted.addAndGet(batchKeywords);
                    skipped.addAndGet(batchSkipped);
                    keywordsProcessed.addAndGet(batchKeywordsProcessed);

                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("导入完成. " +
                "recipes = " + recipesInserted.get() + ", " +
                "keywords = " + keywordsInserted.get() + ", " +
                "keywordsProcessed = " + keywordsProcessed.get() + ", " +
                "skipped = " + skipped.get());

        verifyKeywordsImport();
    }

    public static void importDataWithKeywordsNoBatch(List<String[]> rows) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger recipesInserted = new AtomicInteger(0);
        AtomicInteger keywordsInserted = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);

        int totalRows = rows.size();
        int rowsPerPartition = Math.max(1000, totalRows / THREAD_COUNT);

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < totalRows; i += rowsPerPartition) {
            partitions.add(rows.subList(i, Math.min(i + rowsPerPartition, totalRows)));
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());

        System.out.println("开始并行导入，分区数: " + partitions.size() + ", 每分区约 " + rowsPerPartition + " 行");

        for (int i = 0; i < partitions.size(); i++) {
            final int partitionIndex = i;
            List<String[]> batch = partitions.get(i);

            pool.submit(() -> {
                int batchRecipes = 0;
                int batchKeywords = 0;
                int batchSkipped = 0;

                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                     PreparedStatement recipeStmt = conn.prepareStatement(getInsertSQLWithoutKeywords());
                     PreparedStatement keywordStmt = conn.prepareStatement(
                             "INSERT INTO keywords (recipe_id, keyword_text) VALUES (?, ?)")) {

                    for (String[] cols : batch) {
                        try {
                            List<String> c = Safety.padToExpected(cols);
                            Integer recipeId = Safety.safeInt(c.get(0));

                            if (recipeId == null) {
                                batchSkipped++;
                                continue;
                            }


                            fillPreparedStatementWithoutKeywords(recipeStmt, c);
                            int recipeResult = recipeStmt.executeUpdate();

                            if (recipeResult > 0) {
                                batchRecipes++;

                                String rawKeywords = c.get(10);
                                if (rawKeywords != null && !rawKeywords.trim().isEmpty()) {
                                    List<String> keywords = parseCSVKeywords(rawKeywords);

                                    for (String keyword : keywords) {
                                        if (keyword.length() > 255) {
                                            keyword = keyword.substring(0, 255);
                                        }

                                        keywordStmt.setInt(1, recipeId);
                                        keywordStmt.setString(2, keyword);
                                        int keywordResult = keywordStmt.executeUpdate();

                                        if (keywordResult > 0) {
                                            batchKeywords++;
                                        }
                                    }
                                }
                            }

                            if (batchRecipes > 0 && batchRecipes % 100 == 0) {
                                System.out.println("分区 " + partitionIndex + " 进度: " +
                                        "recipes=" + batchRecipes + ", " +
                                        "keywords=" + batchKeywords);
                            }

                        } catch (Exception ex) {
                            batchSkipped++;

                            System.err.println("插入失败: " + ex.getMessage());
                        }
                    }

                } catch (Exception e) {
                    System.err.println("分区 " + partitionIndex + " 数据库连接失败: " + e.getMessage());
                    batchSkipped = batch.size();
                } finally {
                    recipesInserted.addAndGet(batchRecipes);
                    keywordsInserted.addAndGet(batchKeywords);
                    skipped.addAndGet(batchSkipped);

                    System.out.println("分区 " + partitionIndex + " 完成: " +
                            "recipes = " + batchRecipes + ", " +
                            "keywords = " + batchKeywords + ", " +
                            "跳过=" + batchSkipped);

                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("导入完成. " +
                "recipes = " + recipesInserted.get() + ", " +
                "keywords = " + keywordsInserted.get() + ", " +
                "skipped=" + skipped.get());

        verifyKeywordsImport();
    }

    public static void checkRawKeywordsFromCSV(String csvPath) {

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvPath))
                .withCSVParser(new CSVParserBuilder().build())
                .build()) {

            String[] header = reader.readNext();
            int keywordsIndex = -1;
            for (int i = 0; i < header.length; i++) {
                if ("keywords".equalsIgnoreCase(header[i])) {
                    keywordsIndex = i;
                    break;
                }
            }

            if (keywordsIndex == -1) {
                System.err.println("在 CSV 中找不到 keywords 列");
                return;
            }

            System.out.println("keywords 列索引: " + keywordsIndex);

            String[] line;
            int lineNum = 0;
            while ((line = reader.readNext()) != null && lineNum < 20) {
                lineNum++;
                if (line.length > keywordsIndex) {
                    String rawKeywords = line[keywordsIndex];
                    System.out.println("行 " + lineNum + " - 原始 keywords: " + rawKeywords);

                    if (rawKeywords != null && !rawKeywords.trim().isEmpty()) {
                        List<String> parsed = parseCSVKeywords(rawKeywords);
                        System.out.println("解析结果: " + parsed);
                        System.out.println("数量: " + parsed.size());
                    }
                    System.out.println("---");
                }
            }

        } catch (Exception e) {
            System.err.println("检查 CSV keywords 失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static List<String> parseCSVKeywords(String rawKeywords) {
        List<String> keywords = new ArrayList<>();

        if (rawKeywords == null || rawKeywords.trim().isEmpty()) {
            return keywords;
        }

        String trimmed = rawKeywords.trim();

        try {

            System.out.println("解析 keywords: '" + trimmed + "'");


            if (trimmed.startsWith("c(") && trimmed.endsWith(")")) {
                String content = trimmed.substring(2, trimmed.length() - 1);
                //System.out.println("检测到 c() 格式，内容: '" + content + "'");
                return parseQuotedItems(content);
            }
            // 2. 处理 JSON 数组格式: ["keyword1", "keyword2"]
            else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                String content = trimmed.substring(1, trimmed.length() - 1);
                //System.out.println("   检测到 JSON 数组格式，内容: '" + content + "'");
                return parseQuotedItems(content);
            }
            // 3. 处理三重引号格式: """keyword1, keyword2"""
            else if (trimmed.startsWith("\"\"\"") && trimmed.endsWith("\"\"\"")) {
                String content = trimmed.substring(3, trimmed.length() - 3);
                //System.out.println("   检测到三重引号格式，内容: '" + content + "'");
                String[] items = content.split(",");
                for (String item : items) {
                    String keyword = item.trim();
                    if (!keyword.isEmpty()) {
                        keywords.add(keyword);
                    }
                }
                return keywords;
            }

            else if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                String keyword = trimmed.substring(1, trimmed.length() - 1);
                //System.out.println("   检测到单个引号字符串: '" + keyword + "'");
                keywords.add(keyword);
                return keywords;
            }

            else if (trimmed.contains(",")) {
                //System.out.println("   检测到逗号分隔格式");
                String[] items = trimmed.split(",");
                for (String item : items) {
                    String keyword = item.trim();
                    if (!keyword.isEmpty()) {
                        keywords.add(keyword);
                    }
                }
                return keywords;
            }
            // 6. 单个关键词
            else {
                //System.out.println("   作为单个关键词处理: '" + trimmed + "'");
                keywords.add(trimmed);
                return keywords;
            }

        } catch (Exception e) {
            //System.err.println("解析 keywords 失败: '" + trimmed + "' - " + e.getMessage());
            e.printStackTrace();
            keywords.add(trimmed);
            return keywords;
        }
    }

    public static List<String> parseQuotedItems(String content) {
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
            } else {
                current.append(c);
            }
        }

        String lastItem = current.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }

        return items;
    }

    public static void createKeywordsTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE TABLE IF NOT EXISTS keywords (" +
                    "keyword_id SERIAL PRIMARY KEY, " +
                    "recipe_id INTEGER NOT NULL, " +
                    "keyword_text TEXT, " +
                    "FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id) ON DELETE CASCADE" +
                    ")";
            stmt.execute(sql);

            stmt.execute("CREATE INDEX IF NOT EXISTS idx_keywords_recipe_id ON keywords(recipe_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_keywords_keyword_text ON keywords(keyword_text)");

            System.out.println("keywords 表创建完成");
        }
    }

    public static String getInsertSQLWithoutKeywords() {
        return """
        INSERT INTO recipes (
            recipe_id, recipe_name, author_id, author_name, cook_time, prep_time,
            total_time, date_published, description, recipe_category, 
            recipe_ingredient, aggregated_rating, review_count, calories, fat_content,
            saturated_fat_content, cholesterol_content, sodium_content, carbohydrate_content,
            fiber_content, sugar_content, protein_content, recipe_servings, recipe_yield,
            recipe_instructions, favorite_users
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (recipe_id) DO NOTHING
    """;
    }

    public static void fillPreparedStatementWithoutKeywords(PreparedStatement ps, List<String> c) throws SQLException {
        ps.setInt(1, Safety.safeInt(c.get(0)));
        ps.setString(2, Safety.safeStr(c.get(1)));
        ps.setInt(3, Safety.safeInt(c.get(2)));
        ps.setString(4, Safety.safeStr(c.get(3)));
        ps.setString(5, Safety.safeStr(c.get(4)));
        ps.setString(6, Safety.safeStr(c.get(5)));
        ps.setString(7, Safety.safeStr(c.get(6)));
        ps.setObject(8, Safety.parseTimestamp(c.get(7)));
        ps.setString(9, Safety.safeStr(c.get(8)));
        ps.setString(10, Safety.safeStr(c.get(9)));
        // 跳过 keywords 列 (索引10)
        ps.setString(11, Safety.normalizeArray(c.get(11))); // recipe_ingredient
        ps.setObject(12, Safety.safeDouble(c.get(12)));     // aggregated_rating
        ps.setObject(13, Safety.safeDouble(c.get(13)));     // review_count
        ps.setObject(14, Safety.safeDouble(c.get(14)));     // calories
        ps.setObject(15, Safety.safeDouble(c.get(15)));     // fat_content
        ps.setObject(16, Safety.safeDouble(c.get(16)));     // saturated_fat_content
        ps.setObject(17, Safety.safeDouble(c.get(17)));     // cholesterol_content
        ps.setObject(18, Safety.safeDouble(c.get(18)));     // sodium_content
        ps.setObject(19, Safety.safeDouble(c.get(19)));     // carbohydrate_content
        ps.setObject(20, Safety.safeDouble(c.get(20)));     // fiber_content
        ps.setObject(21, Safety.safeDouble(c.get(21)));     // sugar_content
        ps.setObject(22, Safety.safeDouble(c.get(22)));     // protein_content
        ps.setObject(23, Safety.safeDouble(c.get(23)));     // recipe_servings
        ps.setString(24, Safety.safeStr(c.get(24)));        // recipe_yield
        ps.setString(25, Safety.safeStr(c.get(25)));        // recipe_instructions
        ps.setString(26, Safety.normalizeArray(c.get(26))); // favorite_users
    }


    public static void verifyKeywordsImport() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
             Statement stmt = conn.createStatement()) {

            System.out.println("验证导入结果:");

            // 检查 recipes 数量
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM recipes");
            if (rs.next()) {
                System.out.println("recipes 表记录数: " + rs.getInt(1));
            }
        }
    }
}