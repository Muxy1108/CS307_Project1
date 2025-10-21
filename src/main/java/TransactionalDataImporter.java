import java.sql.Connection;
import java.sql.SQLException;

public class TransactionalDataImporter {

    public static void importAllWithTransaction() {
        Connection conn = null;
        try {
            conn = DatabaseConnection.getConnection();
            conn.setAutoCommit(false); // 开启事务

            // 按依赖顺序导入
            CSVDataImporter.importUsersRobust(conn);
            CSVDataImporter.importRecipesRobust(conn);
            CSVDataImporter.importReviewsRobust(conn);
            CSVDataImporter.importFavorites(conn);
            CSVDataImporter.importLikes(conn);
            CSVDataImporter.importFollows(conn);

            conn.commit(); // 提交事务
            System.out.println("所有数据导入成功！");

        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback(); // 回滚事务
                    System.out.println("导入失败，已回滚所有操作");
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 各个导入方法需要调整以接收Connection参数
   // private static void importUsers(Connection conn) throws Exception {
        // 实现类似上面的importUsers，但使用传入的conn


   // }

    // 其他方法类似...
}