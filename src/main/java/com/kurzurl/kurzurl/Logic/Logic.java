package com.kurzurl.kurzurl.Logic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.kurzurl.kurzurl.controller.IDConverter;


public class Logic {
	
	private static final String table = "url_shortner";
	 private static final String TABLE_CREATE = "CREATE TABLE IF NOT EXISTS " + table + " (" + //
	            "id bigint(20) unsigned NOT NULL AUTO_INCREMENT," + //
	            "long_url VARCHAR(2048) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL," + //
	            "PRIMARY KEY (id)" + //
	            ") ENGINE=InnoDB;";
	 private DataSource dataSource = null;
	    private static final Logger log = Logger.getLogger(Logic.class);
	    private Properties config = null;

	    public Logic() throws IOException {
	        try {
	            init();
	        } catch (IOException e) {
	            throw new IOException("Cannot initiate a DBStore instance.  ", e);
	        }
	    }
    

    public Long getId(String longUrl) throws Exception {
    	Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
	Long id = null;
	try {
	    try {
	    	conn = dataSource.getConnection();
	    	 st = conn.prepareStatement("SELECT id FROM " + table + " WHERE long_url='"+ longUrl.trim() + "'");
	    	 rs = st.executeQuery();;
		if (rs.next()) {
		    id = rs.getLong("id");
		}
	    } finally {

		if (rs != null) {
		    rs.close();
		}
		if (st != null) {
		    st.close();
		}
		if (conn != null) {
		    conn.close();
		}
	    }
	} catch (Exception e) {
	    throw e;
	}
	return id;
    }

    public String getShort(String url) throws Exception {
    	
    	Connection conn = null;
        PreparedStatement st = null;
      
	Long id = getId(url);// check if URL has been shorten already
	if (id != null) {
	    // if id is not null, this link has been shorten already.
	    // nothing to do

	} else {
	    // at this point id is null, make it shorter
		conn = dataSource.getConnection();
	    String sqlInsert = "INSERT INTO "+table+"(long_url) VALUES('"
		    + url.trim() + "')";
	    try {
		
		st = conn.prepareStatement(sqlInsert);
		st.executeUpdate();
	    } finally {
		if (st != null) {
		    st.close();
		}
		if (conn != null) {
		    conn.close();
		}
	    }
	    // after we insert the record, we obtain the ID as identifier of our
	    // new short link
	    id = getId(url);
            
       
	    

	}
	
    String uniqueID = IDConverter.INSTANCE.createUniqueID(id);
    //String baseString = formatLocalURLFromShortener(a);
     
	 return uniqueID;
	
    }
    

    public String getLongUrl(String urlId) throws Exception {
	if (urlId.startsWith("/")) {
	    urlId = urlId.replace("/", "");
	}
	Long dictionaryKey = IDConverter.INSTANCE.getDictionaryKeyFromUniqueID(urlId);
	String query = "SELECT long_url FROM "+table+" where id=" + dictionaryKey;
	String longUrl = null;
	Connection conn = null;
    PreparedStatement st = null;
	ResultSet rs = null;
	

	try {
		 conn = dataSource.getConnection();
	    st = conn.prepareStatement(query);
	    rs = st.executeQuery();
	    if (rs.next()) {
		longUrl = rs.getString("long_url");
	    }
	} finally {

	    if (rs != null) {
		rs.close();
	    }
	    if (st != null) {
		st.close();
	    }
	    if (conn != null) {
		conn.close();
	    }
	}

	return longUrl;
    }
    
    private void closeSilent(ResultSet rSet, PreparedStatement pStmt, Connection conn) {
        try {
            if (rSet != null)
                rSet.close();
            if (pStmt != null)
                pStmt.close();
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            log.warn("DBStore cannot close closeable. ", e);
        }
    }
    private void init() throws IOException {
        config = DataSourceFactory.getProperties();
        try {
            dataSource = DataSourceFactory.createDataSource(config);
        } catch (Exception e) {
            DataSourceFactory.closeDataSource(dataSource);
            throw new IOException(e);
        }
        Connection conn = null;
        Statement stmt;
        PreparedStatement pStmtCreate = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + table + "'");
            if (!rs.next()) {
                // table does not exist so let's create one
                pStmtCreate = conn.prepareStatement(TABLE_CREATE);
                pStmtCreate.executeUpdate();
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            closeSilent(rs, pStmtCreate, conn);
        }
    }

}
