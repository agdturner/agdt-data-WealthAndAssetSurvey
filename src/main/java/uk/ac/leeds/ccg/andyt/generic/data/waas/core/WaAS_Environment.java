/*
 * Copyright 2018 Andy Turner, CCG, University of Leeds.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.core;

import java.io.File;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.handlers.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_Environment extends WaAS_OutOfMemoryErrorHandler {

    public transient final Generic_Environment ge;
    public transient final WaAS_Strings strings;
    public transient final WaAS_Files files;
    public transient final WaAS_HHOLD_Handler hh;
    public transient WaAS_Data data;
    public transient final String EOL = System.getProperty("line.separator");
    public transient final byte W1 = 1;
    public transient final byte W2 = 2;
    public transient final byte W3 = 3;
    public transient final byte W4 = 4;
    public transient final byte W5 = 5;
    
    /**
     * Stores the number of waves in the WaAS
     */
    public transient final byte NWAVES = 5;

    /**
     * Stores the {@link ge} log ID for the log set up for WaAS.
     */
    public transient final int logID;

    public WaAS_Environment() {
        this(new Generic_Environment());
    }
    public WaAS_Environment(Generic_Environment ge) {
        this(ge, ge.files.getDataDir());
    }
    
    public WaAS_Environment(File dataDir) {
        this(new Generic_Environment(), dataDir);
    }
    
    public WaAS_Environment(Generic_Environment ge, File dataDir) {
        this.ge = ge;
        strings = new WaAS_Strings();
        files = new WaAS_Files(strings, dataDir);
        File f = files.getEnvDataFile();
        if (f.exists()) {
            data = (WaAS_Data) ge.io.readObject(f);
            initData();
            //data.env = this;
        } else {
            data = new WaAS_Data(this);
        }
        logID = ge.initLog(strings.s_WaAS);
        hh = new WaAS_HHOLD_Handler(this);
    }

    private void initData() {
        data.env = this;
    }

    /**
     * A method to try to ensure there is enough memory to continue.
     *
     * @return
     */
    @Override
    public boolean checkAndMaybeFreeMemory() {
        System.gc();
        while (getTotalFreeMemory() < Memory_Threshold) {
//            int clear = clearAllData();
//            if (clear == 0) {
//                return false;
//            }
            if (!swapDataAny()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to swap some of {@link #data}.
     *
     * @param hoome handleOutOfMemoryError
     * @return {@code true} iff some data was successfully swapped.
     */
    @Override
    public boolean swapDataAny(boolean hoome) {
        try {
            boolean r = swapDataAny();
            checkAndMaybeFreeMemory();
            return r;
        } catch (OutOfMemoryError e) {
            if (hoome) {
                clearMemoryReserve();
                boolean r = swapDataAny(HOOMEF);
                initMemoryReserve();
                return r;
            } else {
                throw e;
            }
        }
    }

    /**
     * Attempts to swap some of {@link #data}.
     *
     * @return {@code true} iff some data was successfully swapped.
     */
    @Override
    public boolean swapDataAny() {
        boolean r = clearSomeData();
        if (r) {
            return r;
        } else {
            System.out.println("No WaAS data to clear. Do some coding to try "
                    + "to arrange to clear something else if needs be. If the "
                    + "program fails then try providing more memory...");
            return r;
        }
    }

    /**
     * Attempts to clear some of {@link #data} using
     * {@link WaAS_Data#clearSomeData()}.
     *
     * @return {@code true} iff some data was successfully cleared.
     */
    public boolean clearSomeData() {
        return data.clearSomeData();
    }

    /**
     * Attempts to clear all of {@link #data} using
     * {@link WaAS_Data#clearAllData()}.
     *
     * @return The amount of data successfully cleared.
     */
    public int clearAllData() {
        int r = data.clearAllData();
        return r;
    }

    /**
     * For caching {@link #data} to {@link WaAS_Files#getEnvDataFile()}.
     */
    public void cacheData() {
        String m = "cacheData";
        logStartTag(m);
        ge.io.writeObject(data, files.getEnvDataFile());
        logEndTag(m);
    }

    /**
     * For convenience.
     * {@link Generic_Environment#logStartTag(java.lang.String, int)}
     *
     * @param s The tag name.
     */
    public final void logStartTag(String s) {
        ge.logStartTag(s, logID);
    }

    /**
     * 
     * @param s The tag name.
     */
    public final void logStartTagMem(String s) {
        ge.logStartTag(s, logID);
        log("TotalFreeMemory " + getTotalFreeMemory());
    }

    /**
     * For convenience. {@link Generic_Environment#log(java.lang.String, int)}
     *
     * @param s The message to be logged.
     */
    public void log(String s) {
        ge.log(s, logID);
    }

    /**
     * For convenience.
     * {@link Generic_Environment#logEndTag(java.lang.String, int)}
     *
     * @param s The tag name.
     */
    public final void logEndTag(String s) {
        ge.logEndTag(s, logID);
    }
    
    public final void logEndTagMem(String s) {
        log("TotalFreeMemory " + getTotalFreeMemory());
        ge.logEndTag(s, logID);
    }
    
}
