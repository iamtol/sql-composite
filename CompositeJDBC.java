import java.io.FileWriter;
import java.sql.*;
import cs.jdbc.driver.*;
import cs.jdbc.driver.Connection;
import java.io.IOException;


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
    
/*    private void printHeaders(ResultSetMetaData rsmd, int colCount)
        throws SQLException
    {
        int lengths[] = new int[colCount + 1];
        
        for (int i = 1; i <= colCount; i++) {
            String colName = rsmd.getColumnName(i);
            lengths[i] = colName.length();
            System.out.print(colName + ",");
        }
        System.out.println();
       
    }*/
    
    public void query(Connection conn, String table, String filename) 
        throws SQLException, IOException
    {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        
		try {
            FileWriter fw = new FileWriter(filename + ".csv");
            stmt = conn.createStatement();
            stmt.execute("SELECT top 300000 EN_FRST_NM, EN_SURNM FROM IP_SHR.VC_IP\n"
            		+ "where IP_ST_CD = '1'\n"
            		+ "AND VLD_TO_DT = '9999-12-31 23:59:59'\n"
            		+ "AND (PRIM_SEG_CD = 'HN'\n"
            		+ "OR DUAL_SEG_CD = 'HN')\n"
            		+ "AND EN_FRST_NM IS NOT NULL\n"
            		+ "AND EN_SURNM IS NOT NULL;");
            		
            //stmt.execute("SELECT * FROM IP_SHR.VC_IP;");
            rs = stmt.getResultSet();
            rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();

            //printHeaders(rsmd, colCount);

/*           while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    System.out.print(rs.getString(i) + ",");
                }
                System.out.println();
            }*/
            
            for(int i = 1; i <= colCount; i ++){
                fw.append(rs.getMetaData().getColumnLabel(i));
                if(i < colCount) fw.append(',');
                else fw.append('\n');
             }
            
            while (rs.next()) {

                for(int i = 1; i <= colCount; i ++){
                    fw.append(rs.getString(i).trim());
                    if(i < colCount) fw.append(',');
                }
                fw.append('\n');
            }
            fw.flush();
            fw.close();
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
        	System.out.println("Processing...");
            conn = composite.connect("DIH");
            composite.query(conn, "DIH", "outputCSV");
            System.out.println("Completed");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}