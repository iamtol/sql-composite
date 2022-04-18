import java.sql.*;
import cs.jdbc.driver.*;
import cs.jdbc.driver.Connection;

public class CompositeJDBC 
{
    private static final String HOST       = "172.30.138.88";     
    private static final String USERNAME   = "dlp_721.03";
    private static final String PASSWORD   = "p@ssw0rd";
    private static final String DOMAIN     = "composite";
    private static final int    PORT       = 9401;  
    
    public Connection connect(String data_source) 
        throws ClassNotFoundException, SQLException
    {
        Class.forName("cs.jdbc.driver.CompositeDriver");
        String url = "jdbc:compositesw:dbapi@" + HOST + ":" + PORT + 
                     "?domain=" + DOMAIN + "&dataSource=DIH";
        Connection conn = (Connection) DriverManager.getConnection(url, USERNAME, PASSWORD);
        CompositeConnection compConn = (CompositeConnection) conn;
        compConn.clearAllDataSourceCredentials();
        compConn.setDataSourceCredentials(data_source, USERNAME, PASSWORD);
        return conn;
    }
    
    private void printHeaders(ResultSetMetaData rsmd, int colCount)
        throws SQLException
    {
        int lengths[] = new int[colCount + 1];
        
        for (int i = 1; i <= colCount; i++) {
            String colName = rsmd.getColumnName(i);
            lengths[i] = colName.length();
            System.out.print(colName + "\t");
        }
        System.out.println();
        
        for (int i = 1; i <= colCount; i++) {
            String dashes = "----------------".substring(0, lengths[i]);
            System.out.print(dashes + "\t");
        }
        System.out.println();
    }
    
    public void query(Connection conn, String table) 
        throws SQLException
    {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        
        try {
            stmt = conn.createStatement();
            stmt.execute("SELECT top 300000 EN_FRST_NM, EN_SURNM FROM IP_SHR.VC_IP"
            		+ "where IP_ST_CD = '1'"
            		+ "AND VLD_TO_DT = '9999-12-31 23:59:59'"
            		+ "AND (PRIM_SEG_CD = 'HN'"
            		+ "OR DUAL_SEG_CD = 'HN')"
            		+ "AND EN_FRST_NM IS NOT NULL"
            		+ "AND EN_SURNM IS NOT NULL;");

            rs = stmt.getResultSet();
            rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();

            printHeaders(rsmd, colCount);

            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
        finally {
            if (rs   != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
    }
    
    public static void main(String args[]) 
    {
        Connection conn;
        CompositeJDBC composite = new CompositeJDBC();
        
        try {
            conn = composite.connect("IP_SHR.VC_IP");
            composite.query(conn, "DIH");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}