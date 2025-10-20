import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CSVDataImporter {
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        try {
            // 使用事务方式导入
            TransactionalDataImporter.importAllWithTransaction();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 所有方法都接收 Connection 参数
    public static void importUsers(Connection conn) throws SQLException, IOException {
        System.out.println("start to import users data...");

        String sql = "INSERT INTO users (author_id, author_name, gender, age, followers_count, following_count) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (BufferedReader reader = new BufferedReader(new FileReader("data/users.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            reader.readLine(); // 跳过标题行

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                try {
                    pstmt.setInt(1, Integer.parseInt(fields[0].trim()));
                    pstmt.setString(2, fields[1].trim());
                    pstmt.setString(3, fields[2].trim());

                    if (fields.length > 3 && !fields[3].trim().isEmpty()) {
                        pstmt.setInt(4, Integer.parseInt(fields[3].trim()));
                    } else {
                        pstmt.setNull(4, Types.INTEGER);
                    }

                    pstmt.setInt(5, Integer.parseInt(fields[4].trim()));
                    pstmt.setInt(6, Integer.parseInt(fields[5].trim()));

                    pstmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("finish importing " + count + " user records");
                    }
                } catch (Exception e) {
                    System.out.println("jumped lines with error: " + line);
                }
            }

            pstmt.executeBatch();
            System.out.println("importing users data finished,totaling: " + count);
        }
    }

    public static void importRecipes(Connection conn) throws SQLException, IOException {
        System.out.println("start to import recipe data...");

        String sql = "INSERT INTO recipe (recipe_id, recipe_name, author_id, cook_time, calories, protein_content) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (BufferedReader reader = new BufferedReader(new FileReader("data/recipes.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            reader.readLine(); // 跳过标题行

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                try {
                    pstmt.setInt(1, Integer.parseInt(fields[0].trim()));
                    pstmt.setString(2, fields[1].trim());
                    pstmt.setInt(3, Integer.parseInt(fields[2].trim()));
                    pstmt.setInt(4, Integer.parseInt(fields[3].trim()));
                    pstmt.setDouble(5, Double.parseDouble(fields[4].trim()));
                    pstmt.setDouble(6, Double.parseDouble(fields[5].trim()));

                    pstmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("finish importing " + count + " recipe records");
                    }
                } catch (Exception e) {
                    System.out.println("jumped lines with error: " + line);
                }
            }

            pstmt.executeBatch();
            System.out.println("importing recipe data finished,totalin: " + count);
        }
    }

    public static void importReviews(Connection conn) throws SQLException, IOException {
        System.out.println("start to import review data...");

        String sql = "INSERT INTO review (review_id, recipe_id, author_id, rating, review_content, date_submitted, date_modified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (BufferedReader reader = new BufferedReader(new FileReader("data/reviews.csv"));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            reader.readLine(); // 跳过标题行

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                try {
                    pstmt.setInt(1, Integer.parseInt(fields[0].trim()));
                    pstmt.setInt(2, Integer.parseInt(fields[1].trim()));
                    pstmt.setInt(3, Integer.parseInt(fields[2].trim()));
                    pstmt.setInt(4, Integer.parseInt(fields[3].trim()));
                    pstmt.setString(5, fields[4].trim());
                    pstmt.setDate(6, Date.valueOf(fields[5].trim()));

                    if (fields.length > 6 && !fields[6].trim().isEmpty()) {
                        pstmt.setDate(7, Date.valueOf(fields[6].trim()));
                    } else {
                        pstmt.setNull(7, Types.DATE);
                    }

                    pstmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("finish importing " + count + " review records");
                    }
                } catch (Exception e) {
                    System.out.println("jumped lines with erroe: " + line);
                }
            }

            pstmt.executeBatch();
            System.out.println("importing review data finished,totaling:  " + count);
        }
    }

    public static void importFavorites(Connection conn) throws SQLException, IOException {
        System.out.println("start to import favorites data...");

        String sql = "INSERT INTO user_recipe_favorite (author_id, recipe_id, favorite_time) VALUES (?, ?, ?)";

        importRelationTable(conn, sql, "data/user_recipe_favorite.csv", "收藏");
    }

    public static void importLikes(Connection conn) throws SQLException, IOException {
        System.out.println("start to import likes data...");

        String sql = "INSERT INTO user_review_like (author_id, review_id, like_time) VALUES (?, ?, ?)";

        importRelationTable(conn, sql, "data/user_review_like.csv", "点赞");
    }

    public static void importFollows(Connection conn) throws SQLException, IOException {
        System.out.println("start to import follows data...");

        String sql = "INSERT INTO user_follow (follower_id, following_id, follow_time) VALUES (?, ?, ?)";

        importRelationTable(conn, sql, "data/user_follow.csv", "Follows");
    }

    public static void importRelationTable(Connection conn, String sql, String filename, String logName)
            throws SQLException, IOException {

        try (BufferedReader reader = new BufferedReader(new FileReader(filename));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            reader.readLine(); // 跳过标题行

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                try {
                    pstmt.setInt(1, Integer.parseInt(fields[0].trim()));
                    pstmt.setInt(2, Integer.parseInt(fields[1].trim()));

                    // 处理时间字段
                    if (fields[2].contains(":")) {
                        pstmt.setTimestamp(3, Timestamp.valueOf(fields[2].trim()));
                    } else {
                        pstmt.setDate(3, Date.valueOf(fields[2].trim()));
                    }

                    pstmt.addBatch();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("finish importing " + count + " " + logName + "records");
                    }
                } catch (Exception e) {
                    System.out.println("jumped lines with errors: " + line);
                }
            }

            pstmt.executeBatch();
            System.out.println(logName + "data importing finished,totaling: " + count + " records");
        }
    }

    // 非事务版本的独立导入方法（可选）
    public static void importAllWithoutTransaction() throws SQLException, IOException {
        try (Connection conn = DatabaseConnection.getConnection()) {
            importUsers(conn);
            importRecipes(conn);
            importReviews(conn);
            importFavorites(conn);
            importLikes(conn);
            importFollows(conn);
        }
    }
}