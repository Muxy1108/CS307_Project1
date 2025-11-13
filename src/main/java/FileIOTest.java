import java.io.*;
import java.util.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class FileIOTest {

    static class User {
        int authorId, age, followersCount, followingCount;
        String authorName, gender, followerUsers, followingUsers;

        // 新的构造函数，匹配fillPreparedStatementForUsers的字段顺序
        public User(int authorId, String authorName, String gender, int age,
                    int followersCount, int followingCount, String followerUsers, String followingUsers) {
            this.authorId = authorId;
            this.authorName = authorName;
            this.gender = gender;
            this.age = age;
            this.followersCount = followersCount;
            this.followingCount = followingCount;
            this.followerUsers = followerUsers;
            this.followingUsers = followingUsers;
        }
    }

    public static List<User> readCsv(String csvFile) {
        List<User> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line = br.readLine(); // 读取标题行
            while ((line = br.readLine()) != null) {
                List<String> fields = parseCsvLine(line);
                if (fields.size() < 8) continue; // 需要8个字段

                // 按照 fillPreparedStatementForUsers 的字段顺序解析
                int authorId = parseIntSafe(fields.get(0));           // author_id
                String authorName = Safety.safeStr(fields.get(1));    // author_name
                String gender = Safety.safeStr(fields.get(2));        // gender
                int age = parseIntSafe(fields.get(3));                // age
                int followersCount = parseIntSafe(fields.get(4));     // followers_count
                int followingCount = parseIntSafe(fields.get(5));     // following_count
                String followerUsers = Safety.safeStr(fields.get(6)); // follower_users
                String followingUsers = Safety.safeStr(fields.get(7));// following_users

                // 注意：User构造函数需要调整以匹配这个字段顺序
                list.add(new User(authorId, authorName, gender, age, followersCount, followingCount, followerUsers, followingUsers));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    // 需要修改User类来匹配新的字段结构


    // CSV行解析方法（处理引号内的逗号）


    // 安全字符串处理（模仿Safety.safeStr）
    private static String safeStr(String s) {
        return (s == null || "null".equalsIgnoreCase(s)) ? "" : s.trim();
    }
    private static List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder field = new StringBuilder();

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                result.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        result.add(field.toString()); // 最后一个字段
        return result;
    }

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
    }

    private static void writeUsersToFile(List<User> users, String filename) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            for (User u : users) {
                bw.write(u.authorId + "," + u.authorName + "," + u.gender + "," +
                        u.age + "," + u.followersCount + "," + u.followingCount + u.followerUsers + u.followingUsers + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 过滤年龄25-35岁的用户
    private static List<User> filterByAge25_35(List<User> users) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (user.age >= 25 && user.age <= 35) {
                result.add(user);
            }
        }
        return result;
    }

    // 过滤粉丝数>100的用户
    private static List<User> filterByFollowers100(List<User> users) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (user.followersCount > 100) {
                result.add(user);
            }
        }
        return result;
    }

    // 过滤女性用户
    private static List<User> filterFemaleUsers(List<User> users) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if ("Female".equalsIgnoreCase(user.gender) || "F".equalsIgnoreCase(user.gender)) {
                result.add(user);
            }
        }
        return result;
    }

    // 更新30岁以下用户粉丝数+1
    private static List<User> updateAgeUnder30(List<User> users) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (user.age < 30) {
                User updatedUser = new User(
                        user.authorId, user.authorName, user.gender, user.age,
                        user.followersCount, user.followingCount , user.followerUsers,user.followingUsers
                );
                result.add(updatedUser);
            } else {
                result.add(user);
            }
        }
        return result;
    }

    // 删除年龄大于50岁的用户
    private static List<User> deleteAgeOver50(List<User> users) {
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (user.age <= 50) {
                result.add(user);
            }
        }
        return result;
    }

    // 复杂聚合查询：统计各年龄段的平均粉丝数
    private static Map<Integer, Double> complexAggregation(List<User> users) {
        Map<Integer, Integer> ageGroupSum = new HashMap<>();
        Map<Integer, Integer> ageGroupCount = new HashMap<>();
        Map<Integer, Double> ageGroupAvg = new HashMap<>();

        for (User user : users) {
            int ageGroup = (user.age / 10) * 10; // 按10岁分组
            ageGroupSum.put(ageGroup, ageGroupSum.getOrDefault(ageGroup, 0) + user.followersCount);
            ageGroupCount.put(ageGroup, ageGroupCount.getOrDefault(ageGroup, 0) + 1);
        }

        for (Integer ageGroup : ageGroupSum.keySet()) {
            int sum = ageGroupSum.get(ageGroup);
            int count = ageGroupCount.get(ageGroup);
            ageGroupAvg.put(ageGroup, (double) sum / count);
        }

        return ageGroupAvg;
    }

    // 复杂连接查询模拟：找出粉丝数高于平均值的女性用户
    private static List<User> complexJoinSimulation(List<User> users) {
        // 先计算平均粉丝数
        int totalFollowers = 0;
        for (User user : users) {
            totalFollowers += user.followersCount;
        }
        double avgFollowers = (double) totalFollowers / users.size();

        // 找出粉丝数高于平均值的女性用户
        List<User> result = new ArrayList<>();
        for (User user : users) {
            if (("Female".equalsIgnoreCase(user.gender) || "F".equalsIgnoreCase(user.gender))
                    && user.followersCount > avgFollowers) {
                result.add(user);
            }
        }
        return result;
    }

    public static void testFileIO(String csvFile) {
        // 创建Excel工作簿
        Workbook wb = new XSSFWorkbook();

        // 创建性能时间表
        Sheet timeSheet = wb.createSheet("性能时间");
        int timeRowIndex = 0;

        // 创建统计结果表
        Sheet statsSheet = wb.createSheet("统计结果");
        int statsRowIndex = 0;

        // 创建聚合结果表
        Sheet aggSheet = wb.createSheet("聚合分析");
        int aggRowIndex = 0;

        // 性能时间表头
        Row timeHeader = timeSheet.createRow(timeRowIndex++);
        timeHeader.createCell(0).setCellValue("操作类型");
        for (int i = 1; i <= 10; i++) timeHeader.createCell(i).setCellValue("第" + i + "轮(ms)");
        timeHeader.createCell(11).setCellValue("平均时间(ms)");

        // 统计结果表头
        Row statsHeader = statsSheet.createRow(statsRowIndex++);
        statsHeader.createCell(0).setCellValue("测试轮次");
        statsHeader.createCell(1).setCellValue("总用户数");
        statsHeader.createCell(2).setCellValue("年龄25-35岁");
        statsHeader.createCell(3).setCellValue("粉丝数>100");
        statsHeader.createCell(4).setCellValue("女性用户");
        statsHeader.createCell(5).setCellValue("更新30岁以下");
        statsHeader.createCell(6).setCellValue("删除50岁以上");
        statsHeader.createCell(7).setCellValue("高粉丝女性");

        // 聚合分析表头
        Row aggHeader = aggSheet.createRow(aggRowIndex++);
        aggHeader.createCell(0).setCellValue("年龄段");
        aggHeader.createCell(1).setCellValue("平均粉丝数");

        Map<String, List<Double>> opTimes = new LinkedHashMap<>();
        String[] operations = {
                "全表读取", "全表写入",
                "查询_年龄25-35", "查询_粉丝数100", "查询_女性用户",
                "更新_30岁以下",
                "删除_50岁以上",
                "复杂聚合查询",
                "复杂连接模拟"
        };

        for (String op : operations) opTimes.put(op, new ArrayList<>());

        Random rand = new Random();
        List<Map<Integer, Double>> allAggResults = new ArrayList<>();

        for (int run = 0; run < 10; run++) {
            long start;
            List<User> users;

            // 1. 全表查询（读取所有数据）
            start = System.nanoTime();
            users = readCsv(csvFile);
            opTimes.get("全表读取").add((System.nanoTime() - start) / 1_000_000.0);

            // 2. 全表写入
            start = System.nanoTime();
            writeUsersToFile(users, "output_all.csv");
            opTimes.get("全表写入").add((System.nanoTime() - start) / 1_000_000.0);

            // 3. 查询测试：年龄25-35岁的用户
            start = System.nanoTime();
            List<User> age25_35 = filterByAge25_35(users);
            opTimes.get("查询_年龄25-35").add((System.nanoTime() - start) / 1_000_000.0);

            // 4. 查询测试：粉丝数>100的用户
            start = System.nanoTime();
            List<User> followers100 = filterByFollowers100(users);
            opTimes.get("查询_粉丝数100").add((System.nanoTime() - start) / 1_000_000.0);

            // 5. 查询测试：女性用户
            start = System.nanoTime();
            List<User> femaleUsers = filterFemaleUsers(users);
            opTimes.get("查询_女性用户").add((System.nanoTime() - start) / 1_000_000.0);

            // 6. 更新测试：30岁以下用户粉丝+1
            start = System.nanoTime();
            List<User> updatedUsers = updateAgeUnder30(users);
            int updateCount = users.size() - updatedUsers.size();
            users = updatedUsers;
            opTimes.get("更新_30岁以下").add((System.nanoTime() - start) / 1_000_000.0);

            // 写入更新后的数据
            writeUsersToFile(users, "output_updated.csv");

            // 7. 删除测试：删除所有年龄大于50岁的用户
            start = System.nanoTime();
            List<User> afterDelete = deleteAgeOver50(users);
            int deleteCount = users.size() - afterDelete.size();
            users = afterDelete;
            opTimes.get("删除_50岁以上").add((System.nanoTime() - start) / 1_000_000.0);

            // 写入删除后的数据
            writeUsersToFile(users, "output_deleted.csv");

            // 8. 复杂聚合查询：统计各年龄段的平均粉丝数
            start = System.nanoTime();
            Map<Integer, Double> ageGroupStats = complexAggregation(users);
            opTimes.get("复杂聚合查询").add((System.nanoTime() - start) / 1_000_000.0);
            allAggResults.add(ageGroupStats);

            // 9. 复杂连接查询模拟：找出粉丝数高于平均值的女性用户
            start = System.nanoTime();
            List<User> highFollowersFemale = complexJoinSimulation(users);
            opTimes.get("复杂连接模拟").add((System.nanoTime() - start) / 1_000_000.0);

            // 写入统计结果表
            Row statsRow = statsSheet.createRow(statsRowIndex++);
            statsRow.createCell(0).setCellValue("第" + (run + 1) + "轮");
            statsRow.createCell(1).setCellValue(users.size());
            statsRow.createCell(2).setCellValue(age25_35.size());
            statsRow.createCell(3).setCellValue(followers100.size());
            statsRow.createCell(4).setCellValue(femaleUsers.size());
            statsRow.createCell(5).setCellValue(updateCount);
            statsRow.createCell(6).setCellValue(deleteCount);
            statsRow.createCell(7).setCellValue(highFollowersFemale.size());

            System.out.println("第 " + (run + 1) + " 轮测试完成");
        }

        // 写入性能时间表
        for (Map.Entry<String, List<Double>> entry : opTimes.entrySet()) {
            Row row = timeSheet.createRow(timeRowIndex++);
            row.createCell(0).setCellValue(entry.getKey());

            double sum = 0;
            List<Double> times = entry.getValue();
            for (int i = 0; i < times.size(); i++) {
                double val = times.get(i);
                sum += val;
                row.createCell(i + 1).setCellValue(String.format("%.6f", val));
            }
            row.createCell(11).setCellValue(String.format("%.6f", sum / times.size()));
        }

        // 写入聚合分析表（取最后一轮的结果）
        if (!allAggResults.isEmpty()) {
            Map<Integer, Double> lastAggResult = allAggResults.get(allAggResults.size() - 1);
            for (Map.Entry<Integer, Double> entry : lastAggResult.entrySet()) {
                Row row = aggSheet.createRow(aggRowIndex++);
                row.createCell(0).setCellValue(entry.getKey() + "-" + (entry.getKey() + 9) + "岁");
                row.createCell(1).setCellValue(String.format("%.2f", entry.getValue()));
            }
        }

        // 自动调整列宽
        for (int i = 0; i <= 11; i++) {
            timeSheet.autoSizeColumn(i);
            if (i < 8) statsSheet.autoSizeColumn(i);
            if (i < 2) aggSheet.autoSizeColumn(i);
        }

        // 保存Excel文件
        try (FileOutputStream fos = new FileOutputStream("data/FileIO.xlsx")) {
            wb.write(fos);
            System.out.println("✅ 全面测试完成");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 输出最终统计
        System.out.println("\n=== 测试完成 ===");
        System.out.println("结果已保存到: data/FileIO_全面测试结果.xlsx");
        System.out.println("包含三个工作表:");
        System.out.println("1. 性能时间 - 各操作执行时间统计");
        System.out.println("2. 统计结果 - 每轮测试的数据量统计");
        System.out.println("3. 聚合分析 - 年龄段平均粉丝数分析");
    }

    public static void main(String[] args) {
        String csvFile = "data/user.csv";
        testFileIO(csvFile);
    }
}