package com.example.demo;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class Tool {
    private static final Logger LOGGER = Logger.getLogger(Tool.class.getName());

    private static final String BASE_DIR = "DEMO";
    private static final String NAMENODE_HOST = "10.2.22.63";
    private static final int NAMENODE_PORT = 9870;
    private static final String HDFS_USER = "hdfs";
    private static final String HDFS_DIR = "/anhvty";

    private static final Path BASE_PATH = Paths.get(System.getProperty("user.dir"), BASE_DIR);
    private static final Path PENDING_DIR = BASE_PATH.resolve("pending");
    private static final Path SUCCESS_DIR = BASE_PATH.resolve("success");
    private static final Path ERROR_DIR = BASE_PATH.resolve("error");
    private static final Path LOG_DIR = BASE_PATH.resolve("log");

    public static void main(String[] args) {
        initializeDirectories();
        setupLogging();
        startFileProcessing();
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


        String initURL = String.format(
                "http://%s:%d/webhdfs/v1%s?op=CREATE&overwrite=true&user.name=%s",
                NAMENODE_HOST, NAMENODE_PORT, hdfsPath, HDFS_USER
        );

        HttpURLConnection initConn = (HttpURLConnection) new URL(initURL).openConnection();
        initConn.setRequestMethod("PUT");
        initConn.setInstanceFollowRedirects(false);

        int initResponse = initConn.getResponseCode();
        if (initResponse != 307) {
            throw new IOException("Không lấy được URL upload. Mã lỗi: " + initResponse);
        }

        String uploadUrl = initConn.getHeaderField("Location");

        HttpURLConnection uploadConn = (HttpURLConnection) new URL(uploadUrl).openConnection();
        uploadConn.setRequestMethod("PUT");
        uploadConn.setDoOutput(true);

        try (OutputStream os = uploadConn.getOutputStream()) {
            os.write(xmlBytes);
        }

        int uploadResponse = uploadConn.getResponseCode();
        if (uploadResponse != 201) {
            throw new IOException("Upload thất bại. Mã lỗi: " + uploadResponse);
        }

        LOGGER.info("Đã gửi file tới HDFS thành công tại: " + hdfsPath);
    }
}
