package com.example.demo;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class Tool {
    private static final Logger LOGGER = Logger.getLogger(Tool.class.getName());
    private static final String CONFIG_FILE = "config.properties";

    private static String BASE_DIR;
    private static String DOMAIN;
    private static String HDFS_USER;
    private static String HDFS_DIR;
    private static String INTERNAL_KEY;

    private static Path BASE_PATH;
    private static Path PENDING_DIR;
    private static Path SUCCESS_DIR;
    private static Path ERROR_DIR;
    private static Path LOG_DIR;

    public static void main(String[] args) {
        loadConfig();
        initializeDirectories();
        setupLogging();
        startFileProcessing();
    }

    private static void loadConfig() {
        Properties config = new Properties();
        Path configPath = Paths.get(System.getProperty("user.dir"), CONFIG_FILE);
        try (InputStream in = Files.newInputStream(configPath)) {
            config.load(in);
        } catch (IOException e) {
            System.err.println("Không thể đọc file cấu hình từ " + configPath.toAbsolutePath() + ": " + e.getMessage());
            System.exit(1);
        }

        BASE_DIR = config.getProperty("base.dir", "demo");
        DOMAIN = config.getProperty("base.domain");
        HDFS_USER = config.getProperty("hdfs.user");
        HDFS_DIR = config.getProperty("hdfs.dir");
        INTERNAL_KEY = config.getProperty("internal.key");

        if (DOMAIN == null || HDFS_DIR == null) {
            System.err.println("Required configuration missing: base.domain and hdfs.dir");
            System.exit(1);
        }

        BASE_PATH = Paths.get(System.getProperty("user.home"), BASE_DIR);
        PENDING_DIR = BASE_PATH.resolve("pending");
        SUCCESS_DIR = BASE_PATH.resolve("success");
        ERROR_DIR = BASE_PATH.resolve("error");
        LOG_DIR = BASE_PATH.resolve("log");
    }

    private static void initializeDirectories() {
        try {
            Files.createDirectories(PENDING_DIR);
            Files.createDirectories(SUCCESS_DIR);
            Files.createDirectories(ERROR_DIR);
            Files.createDirectories(LOG_DIR);
            System.out.println("Đã tạo thư mục DEMO nếu chưa tồn tại tại: " + BASE_PATH.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("Không thể tạo thư mục DEMO: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void setupLogging() {
        try {
            FileHandler fileHandler = new FileHandler(LOG_DIR.resolve("tool.log").toString(), true);
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileHandler);
            LOGGER.addHandler(new ConsoleHandler());
            LOGGER.setLevel(Level.INFO);
            LOGGER.info("Tool đã khởi động và sẵn sàng làm việc.");
        } catch (IOException e) {
            System.err.println("Không thể thiết lập logging: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void startFileProcessing() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LOGGER.info("Đang kiểm tra file trong thư mục pending...");
                LOGGER.info("Đường dẫn thư mục pending: " + PENDING_DIR.toAbsolutePath());

                File[] files = PENDING_DIR.toFile().listFiles();
                if (files == null || files.length == 0) {
                    LOGGER.info("Không có file nào trong thư mục pending.");
                    return;
                }

                for (File file : files) {
                    if (!file.getName().toLowerCase().endsWith(".xml")) {
                        LOGGER.warning("File " + file.getName() + " không phải định dạng XML. Di chuyển sang error.");
                        Files.move(file.toPath(), ERROR_DIR.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
                        continue;
                    }

                    try {
                        LOGGER.info("Đang xử lý file XML: " + file.getName());
                        byte[] contentBytes = Files.readAllBytes(file.toPath());
                        sendToHadoopApi(file.getName(), contentBytes);
                        Files.move(file.toPath(), SUCCESS_DIR.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
                        LOGGER.info("Gửi file thành công: " + file.getName());
                    } catch (Exception e) {
                        LOGGER.severe("Lỗi khi xử lý file XML " + file.getName() + ": " + e.getMessage());
                        Files.move(file.toPath(), ERROR_DIR.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            } catch (Exception e) {
                LOGGER.severe("Lỗi trong quá trình kiểm tra và gửi file: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    private static void sendToHadoopApi(String fileName, byte[] xmlBytes) throws IOException {
        String sanitizedFileName = fileName.replaceAll("[\\[\\]{}()<>*?|\"^%$#@!~`]", "_");
        String hdfsPath = HDFS_DIR + "/" + sanitizedFileName;

        String initURL = String.format("%s%s?op=CREATE&overwrite=false", DOMAIN, hdfsPath);
        HttpURLConnection initConn = (HttpURLConnection) new URL(initURL).openConnection();
        initConn.setRequestMethod("PUT");
        initConn.setInstanceFollowRedirects(false);  // quan trọng: tắt tự redirect
        if (INTERNAL_KEY != null) {
            initConn.setRequestProperty("Authorization", INTERNAL_KEY);
        }

        int initResponse = initConn.getResponseCode();

        // Kiểm tra mã trả về phải là 307 (Temporary Redirect)
        if (initResponse != 307) {
            throw new IOException("Không lấy được URL upload. Mã lỗi: " + initResponse);
        }

        String uploadUrl = initConn.getHeaderField("Location");
        if (uploadUrl == null || uploadUrl.isEmpty()) {
            throw new IOException("Không có URL upload trả về trong header Location.");
        }

        // Mở kết nối mới tới URL upload thực sự
        HttpURLConnection uploadConn = (HttpURLConnection) new URL(uploadUrl).openConnection();
        uploadConn.setRequestMethod("PUT");
        uploadConn.setDoOutput(true);
        if (HDFS_USER != null) {
            uploadConn.setRequestProperty("Hdfs-User", HDFS_USER);
        }

        // Ghi dữ liệu file XML vào body request
        try (OutputStream os = uploadConn.getOutputStream()) {
            os.write(xmlBytes);
        }

        int uploadResponse = uploadConn.getResponseCode();
        if (uploadResponse != HttpURLConnection.HTTP_CREATED) {  // 201 là thành công upload
            throw new IOException("Upload thất bại. Mã lỗi: " + uploadResponse);
        }

        LOGGER.info("Đã gửi file tới HDFS thành công tại: " + hdfsPath);
    }

}