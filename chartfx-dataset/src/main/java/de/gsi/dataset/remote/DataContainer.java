package de.gsi.dataset.remote;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Data storage container to store image and other (primarily) binary data
 * 
 * @author rstein
 */
@SuppressWarnings("PMD.DataClass") // yes it's a data storage class with no added functionality
public class DataContainer implements Serializable { 
    private static final long serialVersionUID = -4443375672892579564L;
    private String selector; // N.B. first, so that selector can be de-serialised early on
    private String exportName;
    private String category;
    private long updatePeriod;
    private List<Data> data;
    private String rbacToken;
    // end data container
    private long timeStampCreation;
    private long timeStampLastAccess;

    public DataContainer(final String exportNameData, final String mimeType, final byte[] imageByteArray, final int imageByteArraySize) {
        this(genExportName(exportNameData), getCategory(exportNameData), -1l, new Data(exportNameData, mimeType, imageByteArray, imageByteArraySize));
    }
    public DataContainer(final String exportName, final String category, final long updatePeriod, final Data ...data) {
        if (exportName.isBlank()) {
            throw new IllegalArgumentException("exportName must not be blang");
        }
        this.exportName = exportName;
        this.category = category;
        this.updatePeriod = updatePeriod;
        this.data = Collections.unmodifiableList(Arrays.asList(data)); 

        // using deliberately internal server time, since we generate cookies and long-polling out of this
        timeStampCreation = System.currentTimeMillis();
        timeStampLastAccess = timeStampCreation;
    }
    public String getCategory() {
        updateAccess();
        return category;
    }
   
    public List<Data> getData() {
        updateAccess();
        return data;
    }

    /**
     * @return convenience method
     */
    public byte[] getDataByteArray() {
        return getData().get(0).getDataByteArray();
    }

    /**
     * @return convenience method
     */
    public int getDataByteArraySize() {
        return getData().get(0).getDataByteArraySize();
    }

    public String getExportName() {
        return exportName;
    }

    /**
     * @return convenience method
     */
    public String getExportNameData() {
        return getData().get(0).getExportNameData();
    }

    public String getRbacToken() {
        return rbacToken;
    }

    public String getSelector() {
        return selector;
    }

    public long getTimeStampCreation() {
        return timeStampCreation;
    }

    public String getTimeStampCreationString() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.UK).format(new Date(timeStampCreation));
    }

    public long getTimeStampLastAccess() {
        return timeStampLastAccess;
    }

    public long getUpdatePeriod() {
        return updatePeriod;
    }
    
    public void setRbacToken(String rbacToken) {
        updateAccess();
        this.rbacToken = rbacToken;
    }
    
    public void setSelector(String selector) {
        updateAccess();
        this.selector = selector;
    }
    
    public void updateAccess() {
        timeStampLastAccess = System.currentTimeMillis();
    }
    
    /**
     * @return convenience method
     */
    protected String getMimeType() {
        return getData().get(0).getMimeType();
    }
    
    private static String fixPreAndPost(final String name) {
        final String fixedPrefix = (name.startsWith("/") ? name : '/' + name);
        return fixedPrefix.endsWith("/") ? fixedPrefix : fixedPrefix + '/';
    }
    
    private static String genExportName(final String name) {
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blang");
        }
        int p = name.lastIndexOf('/');
        if (p < 0) {
            p = 0;
        }
        int e = name.lastIndexOf('.');
        if (e < 0) {
            e = name.length();
        }
        return name.substring(p, e);
    }
    
    private static String getCategory(final String name) {
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blang");
        }
        final int p = name.lastIndexOf('/');
        if (p < 0) {
            return "";
        }
        return fixPreAndPost(name.substring(0, p+1));
    }
    
}