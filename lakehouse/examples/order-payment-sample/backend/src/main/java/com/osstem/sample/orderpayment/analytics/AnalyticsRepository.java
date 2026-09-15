package com.osstem.sample.orderpayment.analytics;

import com.osstem.sample.orderpayment.config.AnalyticsUnavailableException;
import com.osstem.sample.orderpayment.config.AppProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.springframework.stereotype.Repository;

/**
 * Trino JDBC 조회. SQL 은 서버에 고정하고 파라미터만 받는다. 프런트가 Trino 를 직접 부르지 않는 이유는
 * 자격증명 노출과 CORS 를 피하기 위해서다. 커넥션 풀은 PoC 라 두지 않는다.
 */
@Repository
public class AnalyticsRepository {

    private final AppProperties.Analytics cfg;

    public AnalyticsRepository(AppProperties props) {
        this.cfg = props.analytics();
    }

    public List<Map<String, Object>> query(String sql, Object... params) {
        if (!cfg.enabled()) {
            throw new AnalyticsUnavailableException("조회 기능이 꺼져 있습니다 (app.analytics.enabled=false)");
        }
        Properties p = new Properties();
        p.setProperty("user", cfg.trinoUser());
        try (Connection c = DriverManager.getConnection(cfg.trinoUrl(), p);
             PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        Object v = rs.getObject(i);
                        row.put(md.getColumnLabel(i), v == null ? null : v.toString());
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (SQLException e) {
            throw new AnalyticsUnavailableException("Trino 조회 실패: " + e.getMessage(), e);
        }
    }
}
