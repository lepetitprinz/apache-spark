package ingestion.other.extlib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *  Lists the files based on the criteria
 *  : recursivity, maximum number of files
 */
public class RecursiveExtensionFilteredLister  implements Serializable {
    @SuppressWarnings("unused")
    private static transient Logger log = LoggerFactory
            .getLogger(RecursiveExtensionFilteredLister.class);
    private static final long serialVersionUID = -5751014237854623589L;

    private List<String> extensions;
    private List<File> files;
    private int limit;
    private boolean recursive;
    private boolean hasChanged;
    private String startPath = null;

    public RecursiveExtensionFilteredLister() {
        this.files = new ArrayList<>();
        this.extensions = new ArrayList<>();
        this.recursive = false;
        this.limit = -1;
        this.hasChanged = true;
    }

    /**
     * Adds an extension to filter. Can start with a dot or not.
     * @param extension to filter on.
     */
    public void addExtension(String extension) {
        if (extension.startsWith(".")) {
            this.extensions.add(extension);
        } else {
            this.extensions.add("." + extension);
        }
        this.hasChanged = true;
    }

    /**
     * Checks if the file is matching constraints
     */
    private boolean check(File dir, String name) {
        File f = new File(dir, name);
        if (f.isDirectory()) {
            if (recursive) {
                list0(f);
            }
            return false;
        } else {
            for (String ext : extensions) {
                if (name.toLowerCase().endsWith(ext)) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean dir() {
        if (this.startPath == null) {
            return false;
        }
        return list0(new File(this.startPath));
    }

    /**
     * Returns a list of files based on the criteria given by setters
     * @return a list of files
     */
    public List<File> getFiles() {
        if (this.hasChanged) {
            dir();
            this.hasChanged = false;
        }
        return files;
    }

    private boolean list0(File folder) {
        if (folder == null) {
            return false;
        }
        if (!folder.isDirectory()) {
            return false;
        }

        File[] listOfFiles = folder.listFiles((dir, name) -> check(dir, name));
        if (listOfFiles == null) {
            return true;
        }
        if (limit == -1) {
            this.files.addAll(Arrays.asList(listOfFiles));
        } else {
            int fileCount = this.files.size();
            if (fileCount >= limit) {
                recursive = false;
                return false;
            }

            for (int i = fileCount, j = 0; i < limit
                    && j < listOfFiles.length; i++, j++) {
                this.files.add(listOfFiles[j]);
            }
        }
        return true;
    }
    /**
     * Sets the limit of files you want to list. Default is no limit
     * @param i
     */
    public void setLimit(int i) {
        this.limit = i;
        this.hasChanged = true;
    }

    /**
     * Sets the starting path
     * @param newPath
     */
    public void setPath(String newPath) {
        this.startPath = newPath;
        this.hasChanged = true;
    }

    /**
     * Sets the recursion, default is false
     * @param b
     */
    public void setRecursive(boolean b) {
        this.recursive = b;
        this.hasChanged = true;
    }
}
