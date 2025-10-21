import java.sql.*;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

public class CSVDataImporter {
    private static final int BATCH_SIZE = 1000;
    private static final SimpleDateFormat[] DATE_FORMATS = {
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy/MM/dd")
    };

    static {
        for (SimpleDateFormat format : DATE_FORMATS) {
            format.setLenient(false);
        }
    }

    public static void main(String[] args) {
        try {
            // 先检查数据库表结构
            checkTableStructure();
            // 然后导入数据
            importAllWithTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 检查表结构
    public static void checkTableStructure() throws SQLException {
        try (Connection conn = DatabaseConnection.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();

            // 检查recipe表结构
            ResultSet columns = metaData.getColumns(null, null, "recipe", null);
            System.out.println("Recipe table columns:");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                System.out.println("  " + columnName + " (" + columnType + "(" + columnSize + "))");
            }
            columns.close();

            // 检查其他表结构
            String[] tables = {"users", "review", "user_recipe_favorite", "user_review_like", "user_follow"};
            for (String table : tables) {
                System.out.println("\n" + table + " table columns:");
                columns = metaData.getColumns(null, null, table, null);
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String columnType = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");
                    System.out.println("  " + columnName + " (" + columnType + "(" + columnSize + "))");
                }
                columns.close();
            }
        }
    }

    // 事务方式导入所有数据
    public static void importAllWithTransaction() throws SQLException, IOException, CsvValidationException  {
        try (Connection conn = DatabaseConnection.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // 先清空表（可选）
                // clearTables(conn);

                importUsersRobust(conn);
                importRecipesRobust(conn);
                importReviewsRobust(conn);
                importFavorites(conn);
                importLikes(conn);
                importFollows(conn);

                conn.commit();
                System.out.println("All data imported successfully!");
            } catch (BatchUpdateException bue) {
                conn.rollback();
                System.err.println("❌ Batch update failed!");
                System.err.println("SQLState: " + bue.getSQLState());
                System.err.println("ErrorCode: " + bue.getErrorCode());
                System.err.println("Message: " + bue.getMessage());
                bue.printStackTrace();
                throw bue; // rethrow so upper layers see it
            }
            catch (Exception e) {
                conn.rollback();
                System.err.println("Transaction rolled back due to error: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }
        catch (Exception outer) {
            System.err.println("❌ Fatal connection or commit error: " + outer.getMessage());
            outer.printStackTrace();
            throw outer;
        }
    }

    // 清空表（可选）
    private static void clearTables(Connection conn) throws SQLException {
        String[] tables = {
                "user_follow", "user_review_like", "user_recipe_favorite",
                "review", "recipe", "users"
        };

        try (Statement stmt = conn.createStatement()) {
            // 禁用外键约束检查
            stmt.execute("SET CONSTRAINTS ALL DEFERRED");

            for (String table : tables) {
                stmt.execute("DELETE FROM " + table);
                System.out.println("Cleared table: " + table);
            }
        }
    }

    // 健壮的用户数据导入
    public static void importUsersRobust(Connection conn) throws SQLException, IOException, CsvValidationException  {
        System.out.println("start to import users data...");

        String sql = "INSERT INTO users (author_id, author_name, gender, age, followers_count, following_count) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        int count = 0;
        int errorCount = 0;
        int successCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("data/user.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line = br.readLine(); // 跳过标题行
            System.out.println("Users CSV header: " + line);

            while ((line = br.readLine()) != null) {
                count++;
                try {
                    String[] fields = parseCSVLineRobust(line);

                    if (fields.length < 6) {
                        System.out.println("Skipping incomplete user record at line " + count + ", fields: " + fields.length);
                        errorCount++;
                        continue;
                    }

                    // 验证必需字段
                    String authorIdStr = cleanField(fields[0]);
                    if (authorIdStr.isEmpty() || !isNumeric(authorIdStr)) {
                        System.out.println("Skipping user with invalid ID at line " + count + ": " + authorIdStr);
                        errorCount++;
                        continue;
                    }

                    pstmt.setInt(1, safeParseInt(authorIdStr)); // author_id
                    pstmt.setString(2, truncateField(fields[1], 100)); // author_name
                    pstmt.setString(3, truncateField(fields[2], 10)); // gender

                    // 处理年龄字段
                    String ageStr = cleanField(fields[3]);
                    if (!ageStr.isEmpty() && isNumeric(ageStr)) {
                        int age = safeParseInt(ageStr);
                        if (age > 0 && age < 150) {
                            pstmt.setInt(4, age);
                        } else {
                            pstmt.setNull(4, Types.INTEGER);
                        }
                    } else {
                        pstmt.setNull(4, Types.INTEGER);
                    }

                    // 处理关注者数量
                    String followersStr = cleanField(fields[4]);
                    if (!followersStr.isEmpty() && isNumeric(followersStr)) {
                        pstmt.setInt(5, Math.max(0, safeParseInt(followersStr)));
                    } else {
                        pstmt.setInt(5, 0);
                    }

                    String followingStr = cleanField(fields[5]);
                    if (!followingStr.isEmpty() && isNumeric(followingStr)) {
                        pstmt.setInt(6, Math.max(0, safeParseInt(followingStr)));
                    } else {
                        pstmt.setInt(6, 0);
                    }

                    pstmt.addBatch();
                    successCount++;

                    if (successCount % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("Users progress: " + count + " lines read, " + successCount + " successful, " + errorCount + " errors");
                    }
                } catch (Exception e) {
                    System.out.println("Error processing user record at line " + count + ": " + e.getMessage());
                    errorCount++;
                }
            }

            pstmt.executeBatch();
            System.out.println("Users import finished: " + successCount + " successful, " + errorCount + " errors, " + count + " total lines read");
        }
    }

    // 健壮的食谱导入方法 - 根据实际表结构调整
    public static void importRecipesRobust(Connection conn) throws IOException, SQLException, CsvValidationException {
        String csvFile = "data/recipes.csv"; // adjust path if needed

        // Use a parser that handles commas inside quotes and double-quote escapes
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .withEscapeChar('"')      // OpenCSV handles "" inside quotes
                .withStrictQuotes(false)  // allows whitespace outside quotes
                .withIgnoreQuotations(false)
                .build();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFile))
                .withCSVParser(parser)
                .withSkipLines(1) // skip header
                .build()) {

            String insertSQL = "INSERT INTO recipe (" +
                    "recipe_id, name, author_id, author_name, cook_time, prep_time, total_time, " +
                    "date_published, description, recipe_category, keywords, recipe_ingredient_parts, " +
                    "aggregated_rating, review_count, calories, fat_content, saturated_fat_content, " +
                    "cholesterol_content, sodium_content, carbohydrate_content, fiber_content, sugar_content, " +
                    "protein_content, recipe_servings, recipe_yield, recipe_instructions, favorite_users" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (recipe_id) DO NOTHING";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                String[] line;
                int lineNum = 1;

                while ((line = reader.readNext()) != null) {
                    lineNum++;

                    // Some rows may not have all fields → skip invalid ones
                    if (line.length < 26) {
                        System.err.println("Skipping line " + lineNum + ": insufficient columns (" + line.length + ")");
                        continue;
                    }

                    try {
                        pstmt.setInt(1, parseInt(line[0]));
                        pstmt.setString(2, safe(line[1]));
                        pstmt.setInt(3, parseInt(line[2]));
                        pstmt.setString(4, safe(line[3]));
                        pstmt.setString(5, safe(line[4]));
                        pstmt.setString(6, safe(line[5]));
                        pstmt.setString(7, safe(line[6]));
                        pstmt.setTimestamp(8, parseTimestamp(line[7]));
                        pstmt.setString(9, safe(line[8]));
                        pstmt.setString(10, safe(line[9]));
                        pstmt.setString(11, safe(line[10]));
                        pstmt.setString(12, safe(line[11]));
                        pstmt.setDouble(13, parseDouble(line[12]));
                        pstmt.setDouble(14, parseDouble(line[13]));
                        pstmt.setDouble(15, parseDouble(line[14]));
                        pstmt.setDouble(16, parseDouble(line[15]));
                        pstmt.setDouble(17, parseDouble(line[16]));
                        pstmt.setDouble(18, parseDouble(line[17]));
                        pstmt.setDouble(19, parseDouble(line[18]));
                        pstmt.setDouble(20, parseDouble(line[19]));
                        pstmt.setDouble(21, parseDouble(line[20]));
                        pstmt.setDouble(22, parseDouble(line[21]));
                        pstmt.setDouble(23, parseDouble(line[22]));
                        pstmt.setDouble(24, parseDouble(line[23]));
                        pstmt.setString(25, safe(line[24]));
                        pstmt.setString(26, safe(line[25]));

                        pstmt.addBatch();
                    } catch (Exception e) {
                        System.err.println("Error parsing line " + lineNum + ": " + e.getMessage());
                    }
                }

                pstmt.executeBatch();
                System.out.println("Recipes imported successfully!");
            }
        }
    }

    // 设置食谱字段值 - 调整后的版本（移除了name字段）
    private static void setRecipeFields(PreparedStatement pstmt, String[] fields) throws SQLException {
        int paramIndex = 1;

        // 基本字段 (0-2)
        setIntSafely(pstmt, paramIndex++, fields[0]); // RecipeId

        // 检查author_id是否有效
        String authorIdStr = cleanField(fields[2]);
        if (authorIdStr.isEmpty() || !isNumeric(authorIdStr)) {
            pstmt.setNull(paramIndex++, Types.INTEGER);
        } else {
            pstmt.setInt(paramIndex++, safeParseInt(authorIdStr));
        }

        pstmt.setString(paramIndex++, truncateField(fields[3], 200)); // AuthorName

        // 时间字段 (4-6)
        pstmt.setString(paramIndex++, truncateField(fields[4], 100)); // CookTime
        pstmt.setString(paramIndex++, truncateField(fields[5], 100)); // PrepTime
        pstmt.setString(paramIndex++, truncateField(fields[6], 100)); // TotalTime

        // 日期字段 (7) - 更宽松的处理
        String dateField = cleanField(fields[7]);
        if (isValidDate(dateField)) {
            setTimestampSafely(pstmt, paramIndex, dateField);
        } else {
            pstmt.setNull(paramIndex, Types.TIMESTAMP);
        }
        paramIndex++;

        // 描述和分类字段 (8-11)
        pstmt.setString(paramIndex++, truncateField(fields[8], 2000)); // Description
        pstmt.setString(paramIndex++, truncateField(fields[9], 200)); // RecipeCategory
        pstmt.setString(paramIndex++, truncateField(fields[10], 1000)); // Keywords
        pstmt.setString(paramIndex++, truncateField(fields[11], 4000)); // RecipeIngredientParts

        // 评分和计数字段 (12-13)
        setDoubleSafely(pstmt, paramIndex++, fields[12]); // AggregatedRating
        setIntSafely(pstmt, paramIndex++, fields[13]); // ReviewCount

        // 营养成分字段 (14-22)
        setDoubleSafely(pstmt, paramIndex++, fields[14]); // Calories
        setDoubleSafely(pstmt, paramIndex++, fields[15]); // FatContent
        setDoubleSafely(pstmt, paramIndex++, fields[16]); // SaturatedFatContent
        setDoubleSafely(pstmt, paramIndex++, fields[17]); // CholesterolContent
        setDoubleSafely(pstmt, paramIndex++, fields[18]); // SodiumContent
        setDoubleSafely(pstmt, paramIndex++, fields[19]); // CarbohydrateContent
        setDoubleSafely(pstmt, paramIndex++, fields[20]); // FiberContent
        setDoubleSafely(pstmt, paramIndex++, fields[21]); // SugarContent
        setDoubleSafely(pstmt, paramIndex++, fields[22]); // ProteinContent

        // 份量和制作说明 (23-25)
        setIntSafely(pstmt, paramIndex++, fields[23]); // RecipeServings
        pstmt.setString(paramIndex++, truncateField(fields[24], 500)); // RecipeYield
        pstmt.setString(paramIndex++, truncateField(fields[25], 8000)); // RecipeInstructions
    }

    // 健壮的评论导入方法
    public static void importReviewsRobust(Connection conn) throws SQLException, IOException, CsvValidationException{
        System.out.println("start to import review data...");

        String sql = "INSERT INTO review (review_id, recipe_id, author_id, author_name, rating, review_content, " +
                "date_submitted, date_modified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        int count = 0;
        int errorCount = 0;
        int successCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("data/reviews.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line = br.readLine(); // 跳过标题行
            System.out.println("Reviews CSV header: " + line);

            while ((line = br.readLine()) != null) {
                count++;
                try {
                    String[] fields = parseCSVLineRobust(line);

                    if (fields.length < 8) {
                        System.out.println("Skipping incomplete review record at line " + count + ", fields: " + fields.length);
                        errorCount++;
                        continue;
                    }

                    // 验证必需字段
                    String reviewIdStr = cleanField(fields[0]);
                    if (reviewIdStr.isEmpty() || !isNumeric(reviewIdStr)) {
                        System.out.println("Skipping review with invalid ID at line " + count + ": " + reviewIdStr);
                        errorCount++;
                        continue;
                    }

                    setIntSafely(pstmt, 1, fields[0]); // ReviewId
                    setIntSafely(pstmt, 2, fields[1]); // RecipeId
                    setIntSafely(pstmt, 3, fields[2]); // AuthorId
                    pstmt.setString(4, truncateField(fields[3], 100)); // AuthorName

                    // 评分字段 - 确保在合理范围内
                    String ratingStr = cleanField(fields[4]);
                    if (!ratingStr.isEmpty() && isNumeric(ratingStr)) {
                        int rating = safeParseInt(ratingStr);
                        if (rating >= 1 && rating <= 5) {
                            pstmt.setInt(5, rating);
                        } else {
                            pstmt.setNull(5, Types.INTEGER);
                        }
                    } else {
                        pstmt.setNull(5, Types.INTEGER);
                    }

                    pstmt.setString(6, truncateField(fields[5], 2000)); // Review

                    // 日期字段
                    setTimestampSafely(pstmt, 7, fields[6]); // DateSubmitted
                    setTimestampSafely(pstmt, 8, fields[7]); // DateModified

                    pstmt.addBatch();
                    successCount++;

                    if (successCount % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("Reviews progress: " + count + " lines read, " + successCount + " successful, " + errorCount + " errors");
                    }
                } catch (Exception e) {
                    System.out.println("Error processing review record at line " + count + ": " + e.getMessage());
                    errorCount++;
                }
            }

            pstmt.executeBatch();
            System.out.println("Reviews import finished: " + successCount + " successful, " + errorCount + " errors, " + count + " total lines read");
        }
    }

    // 导入收藏关系
    public static void importFavorites(Connection conn) throws SQLException, IOException, CsvValidationException{
        System.out.println("start to import favorites data...");

        String sql = "INSERT INTO user_recipe_favorite (author_id, recipe_id, favorite_time) VALUES (?, ?, ?)";

        int count = 0;
        int errorCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("data/recipes.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line = br.readLine(); // 跳过标题行
            int lineCount = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;
                try {
                    String[] fields = parseCSVLineRobust(line);

                    if (fields.length > 26 && !cleanField(fields[26]).isEmpty()) {
                        String favoriteUsers = cleanField(fields[26]); // FavoriteUsers字段
                        String recipeIdStr = cleanField(fields[0]);

                        if (recipeIdStr.isEmpty() || !isNumeric(recipeIdStr)) {
                            continue;
                        }

                        int recipeId = safeParseInt(recipeIdStr);

                        // 解析收藏用户列表
                        if (favoriteUsers.matches(".*\\d.*")) {
                            String[] userIds = favoriteUsers.split(",");
                            for (String userIdStr : userIds) {
                                try {
                                    String cleanUserId = cleanField(userIdStr);
                                    if (!cleanUserId.isEmpty() && isNumeric(cleanUserId)) {
                                        int userId = safeParseInt(cleanUserId);
                                        pstmt.setInt(1, userId);
                                        pstmt.setInt(2, recipeId);
                                        pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                                        pstmt.addBatch();
                                        count++;

                                        if (count % BATCH_SIZE == 0) {
                                            pstmt.executeBatch();
                                        }
                                    }
                                } catch (NumberFormatException e) {
                                    // 跳过非数字的用户ID
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error processing favorites at line " + lineCount + ": " + e.getMessage());
                    errorCount++;
                }
            }

            pstmt.executeBatch();
            System.out.println("Favorites data importing finished: " + count + " records, " + errorCount + " errors");
        }
    }

    // 导入点赞关系
    public static void importLikes(Connection conn) throws SQLException, IOException, CsvValidationException {
        System.out.println("start to import likes data...");

        String sql = "INSERT INTO user_review_like (author_id, review_id, like_time) VALUES (?, ?, ?)";

        int count = 0;
        int errorCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("data/reviews.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line = br.readLine(); // 跳过标题行
            int lineCount = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;
                try {
                    String[] fields = parseCSVLineRobust(line);

                    if (fields.length > 8 && !cleanField(fields[8]).isEmpty()) {
                        String likes = cleanField(fields[8]); // Likes字段
                        String reviewIdStr = cleanField(fields[0]);

                        if (reviewIdStr.isEmpty() || !isNumeric(reviewIdStr)) {
                            continue;
                        }

                        int reviewId = safeParseInt(reviewIdStr);

                        // 解析点赞用户列表
                        if (likes.matches(".*\\d.*")) {
                            String[] userIds = likes.split(",");
                            for (String userIdStr : userIds) {
                                try {
                                    String cleanUserId = cleanField(userIdStr);
                                    if (!cleanUserId.isEmpty() && isNumeric(cleanUserId)) {
                                        int userId = safeParseInt(cleanUserId);
                                        pstmt.setInt(1, userId);
                                        pstmt.setInt(2, reviewId);
                                        pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                                        pstmt.addBatch();
                                        count++;

                                        if (count % BATCH_SIZE == 0) {
                                            pstmt.executeBatch();
                                        }
                                    }
                                } catch (NumberFormatException e) {
                                    // 跳过非数字的用户ID
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error processing likes at line " + lineCount + ": " + e.getMessage());
                    errorCount++;
                }
            }

            pstmt.executeBatch();
            System.out.println("Likes data importing finished: " + count + " records, " + errorCount + " errors");
        }
    }

    // 导入关注关系
    public static void importFollows(Connection conn) throws SQLException, IOException, CsvValidationException {
        System.out.println("start to import follows data...");

        String sql = "INSERT INTO user_follow (follower_id, following_id, follow_time) VALUES (?, ?, ?)";

        int count = 0;
        int errorCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("data/user.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line = br.readLine(); // 跳过标题行
            int lineCount = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;
                try {
                    String[] fields = parseCSVLineRobust(line);

                    if (fields.length >= 7) {
                        String currentUserIdStr = cleanField(fields[0]);
                        if (currentUserIdStr.isEmpty() || !isNumeric(currentUserIdStr)) {
                            continue;
                        }

                        int currentUserId = safeParseInt(currentUserIdStr);

                        // 处理关注者（FollowerUsers）
                        if (fields.length > 6 && !cleanField(fields[6]).isEmpty()) {
                            String followerUsers = cleanField(fields[6]);
                            if (followerUsers.matches(".*\\d.*")) {
                                String[] followerIds = followerUsers.split(",");
                                for (String followerIdStr : followerIds) {
                                    try {
                                        String cleanFollowerId = cleanField(followerIdStr);
                                        if (!cleanFollowerId.isEmpty() && isNumeric(cleanFollowerId)) {
                                            int followerId = safeParseInt(cleanFollowerId);
                                            pstmt.setInt(1, followerId);
                                            pstmt.setInt(2, currentUserId);
                                            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                                            pstmt.addBatch();
                                            count++;
                                        }
                                    } catch (NumberFormatException e) {
                                        // 跳过非数字的用户ID
                                    }
                                }
                            }
                        }

                        // 处理正在关注（FollowingUsers）
                        if (fields.length > 7 && !cleanField(fields[7]).isEmpty()) {
                            String followingUsers = cleanField(fields[7]);
                            if (followingUsers.matches(".*\\d.*")) {
                                String[] followingIds = followingUsers.split(",");
                                for (String followingIdStr : followingIds) {
                                    try {
                                        String cleanFollowingId = cleanField(followingIdStr);
                                        if (!cleanFollowingId.isEmpty() && isNumeric(cleanFollowingId)) {
                                            int followingId = safeParseInt(cleanFollowingId);
                                            pstmt.setInt(1, currentUserId);
                                            pstmt.setInt(2, followingId);
                                            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                                            pstmt.addBatch();
                                            count++;
                                        }
                                    } catch (NumberFormatException e) {
                                        // 跳过非数字的用户ID
                                    }
                                }
                            }
                        }

                        if (count % BATCH_SIZE == 0) {
                            pstmt.executeBatch();
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error processing follows at line " + lineCount + ": " + e.getMessage());
                    errorCount++;
                }
            }

            pstmt.executeBatch();
            System.out.println("Follows data importing finished: " + count + " records, " + errorCount + " errors");
        }
    }

    // 健壮的CSV行解析
    private static String[] parseCSVLineRobust(String line) {
        if (line == null || line.trim().isEmpty()) {
            return new String[0];
        }

        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;
        boolean lastCharWasQuote = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes) {
                    if (lastCharWasQuote) {
                        // 双引号转义
                        field.append('"');
                        lastCharWasQuote = false;
                    } else {
                        lastCharWasQuote = true;
                    }
                } else {
                    inQuotes = true;
                }
            } else {
                if (lastCharWasQuote) {
                    // 结束引号字段
                    inQuotes = false;
                    lastCharWasQuote = false;
                }

                if (c == ',' && !inQuotes) {
                    fields.add(field.toString());
                    field.setLength(0);
                } else {
                    field.append(c);
                }
            }
        }

        // 添加最后一个字段
        fields.add(field.toString());

        return fields.toArray(new String[0]);
    }

    // 检查是否为有效日期
    private static boolean isValidDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return false;
        }

        String cleaned = cleanField(dateStr);

        // 检查常见的非日期字符串
        if (cleaned.matches(".*[a-zA-Z].*") &&
                !cleaned.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            return false;
        }

        // 尝试解析
        for (SimpleDateFormat format : DATE_FORMATS) {
            try {
                format.parse(cleaned);
                return true;
            } catch (Exception e) {
                // 继续尝试下一种格式
            }
        }

        return false;
    }

    // 填充缺失字段的方法
    private static String[] padFields(String[] original, int targetLength) {
        String[] padded = new String[targetLength];
        System.arraycopy(original, 0, padded, 0, Math.min(original.length, targetLength));
        for (int i = original.length; i < targetLength; i++) {
            padded[i] = "";
        }
        return padded;
    }

    // 增强的清理字段方法
    private static String cleanField(String field) {
        if (field == null) return "";

        field = field.trim();

        // 移除多余的引号
        if (field.startsWith("\"") && field.endsWith("\"")) {
            field = field.substring(1, field.length() - 1);
        }

        // 移除R语言格式
        if (field.startsWith("c(\"") && field.endsWith("\")")) {
            field = field.substring(3, field.length() - 2);
        }

        // 清理特殊字符
        field = field.replace("\"\"", "\"") // 处理转义引号
                .replace("\\\"", "\"")
                .replace("\\n", " ")
                .replace("\\r", " ")
                .replace("\\t", " ")
                .replace("&rsquo;", "'")
                .replace("&amp;", "&")
                .replace("&quot;", "\"")
                .replace("&lt;", "<")
                .replace("&gt;", ">");

        return field;
    }

    // 截断字段到指定长度
    private static String truncateField(String field, int maxLength) {
        String cleaned = cleanField(field);
        if (cleaned.length() > maxLength) {
            return cleaned.substring(0, maxLength);
        }
        return cleaned;
    }

    // 安全的整数解析
    private static int safeParseInt(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            String numericPart = extractNumericPart(value);
            if (numericPart.isEmpty()) {
                return 0;
            }
            if (numericPart.contains(".")) {
                return (int) Math.round(Double.parseDouble(numericPart));
            }
            return Integer.parseInt(numericPart);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // 安全的双精度解析
    private static double safeParseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0;
        }
        try {
            String numericPart = extractNumericPart(value);
            if (numericPart.isEmpty()) {
                return 0.0;
            }
            return Double.parseDouble(numericPart);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    // 提取字符串中的数字部分
    private static String extractNumericPart(String value) {
        if (value == null) return "";

        Pattern pattern = Pattern.compile("-?\\d+\\.?\\d*");
        Matcher matcher = pattern.matcher(value.trim());

        if (matcher.find()) {
            return matcher.group();
        }
        return "";
    }

    // 检查是否为数字
    private static boolean isNumeric(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        return str.trim().matches("-?\\d+(\\.\\d+)?");
    }

    // 安全设置整数字段
    private static void setIntSafely(PreparedStatement pstmt, int index, String value) throws SQLException {
        String cleaned = cleanField(value);
        if (cleaned.isEmpty() || !isNumeric(cleaned)) {
            pstmt.setNull(index, Types.INTEGER);
        } else {
            pstmt.setInt(index, safeParseInt(cleaned));
        }
    }

    // 安全设置双精度字段
    private static void setDoubleSafely(PreparedStatement pstmt, int index, String value) throws SQLException {
        String cleaned = cleanField(value);
        if (cleaned.isEmpty() || !isNumeric(cleaned)) {
            pstmt.setNull(index, Types.DOUBLE);
        } else {
            pstmt.setDouble(index, safeParseDouble(cleaned));
        }
    }

    // 安全设置时间戳字段
    private static void setTimestampSafely(PreparedStatement pstmt, int index, String value) throws SQLException {
        String cleaned = cleanField(value);
        if (cleaned.isEmpty() || !isValidDate(cleaned)) {
            pstmt.setNull(index, Types.TIMESTAMP);
            return;
        }

        try {
            for (SimpleDateFormat format : DATE_FORMATS) {
                try {
                    Date date = format.parse(cleaned);
                    pstmt.setTimestamp(index, new Timestamp(date.getTime()));
                    return;
                } catch (Exception e) {
                    // 继续尝试
                }
            }
            pstmt.setNull(index, Types.TIMESTAMP);
        } catch (Exception e) {
            pstmt.setNull(index, Types.TIMESTAMP);
        }
    }

    // 非事务版本的独立导入方法
    public static void importAllWithoutTransaction() throws SQLException, IOException, CsvValidationException{
        try (Connection conn = DatabaseConnection.getConnection()) {
            importUsersRobust(conn);
            importRecipesRobust(conn);
            importReviewsRobust(conn);
            importFavorites(conn);
            importLikes(conn);
            importFollows(conn);
        }
    }

    private static String safe(String s) {
        return (s == null || s.trim().isEmpty()) ? null : s.trim();
    }

    private static Integer parseInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return null; }
    }

    private static Double parseDouble(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return null; }
    }

    private static Timestamp parseTimestamp(String s) {
        try {
            return Timestamp.valueOf(s.replace("T", " ").replace("Z", ""));
        } catch (Exception e) {
            return null;
        }
    }
}