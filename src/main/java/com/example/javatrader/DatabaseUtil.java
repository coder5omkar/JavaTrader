package com.example.javatrader;
import java.sql.*;
import java.time.LocalDateTime;

public class DatabaseUtil {
    private static final String URL = "jdbc:mysql://localhost:3306/dhanhq_db";
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    public static void saveToDB(String table, int securityId, String name, double ltp, LocalDateTime ltt) {
        String query = "INSERT INTO " + table + " (security_id, security_name, ltp, ltt) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setInt(1, securityId);
            ps.setString(2, name);
            ps.setDouble(3, ltp);
            ps.setTimestamp(4, Timestamp.valueOf(ltt));
            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
