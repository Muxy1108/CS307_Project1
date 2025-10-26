import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CSVReader {

    public static List<String[]> ReadCSV(String csvPath,int EXPECTED_COLUMNS){
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
             com.opencsv.CSVReader reader = new CSVReaderBuilder(br)
                     .withCSVParser(parser)
                     .withSkipLines(0)
                     .build()) {

            String[] header = reader.readNext();
            if (header == null) {
                System.err.println("Empty CSV file.");
                return rows;
            }

            String[] line;
            int lineCount = 0;
            int errorCount = 0;

            while(true){
                try {
                    line = reader.readNext();
                    if(line == null) break;

                    lineCount++;

                    if (line.length >= EXPECTED_COLUMNS) {
                        rows.add(line);
                    }else{
                        errorCount++;
                    }

                    if (lineCount % 10000 == 0) {
                        System.out.println("已读取行数: " + lineCount + ", 错误: " + errorCount);
                    }

                } catch (Exception e) {
                    errorCount ++;
                }
            }
            System.out.println("读取完成: 总行数 = " + lineCount + ", 有效行 = " + rows.size() + ", 错误 = " + errorCount);
        } catch (Exception e) {
            System.err.println("读取 CSV 文件失败: " + e.getMessage());
            e.printStackTrace();
        }
        return rows;
    }
}
