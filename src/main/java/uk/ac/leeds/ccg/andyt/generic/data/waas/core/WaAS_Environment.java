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
import java.io.PrintWriter;
import java.io.Serializable;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
//import uk.ac.leeds.ccg.andyt.data.postcode.Generic_UKPostcode_Handler;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_Environment extends WaAS_OutOfMemoryErrorHandler
        implements Serializable {

    public transient Generic_Environment ge;
    public transient WaAS_Files files;
    public final WaAS_HHOLD_Handler hh;
    public WaAS_Data data;
    public transient static final String EOL = System.getProperty("line.separator");
    
    /**
     * Stores the {@link ge} log ID for the log set up for WaAS.
     */
    protected final int logID;

    /**
     * A convenience method for logging.
     * @param s The message to be logged.
     */
    public void log(String s) {
        ge.log(s, logID);
    }

    /**
     * A convenience method for logging.
     * @param s The message to be logged in a start tag.
     */
    public void logStartTag(String s) {
        ge.logStartTag(s, logID);
    }

    /**
     * A convenience method for logging.
     * @param s The message to be logged in an end tag.
     */
    public void logEndTag(String s) {
        ge.logEndTag(s, logID);
    }

     public WaAS_Environment(Generic_Environment ge) {
        //Memory_Threshold = 3000000000L;
        files = new WaAS_Files(ge.getFiles().getDataDir());
        File f;
        f = files.getEnvDataFile();
        if (f.exists()) {
            loadData();
            //data.files = files;
            //data.strings = strings;
        } else {
            data = new WaAS_Data(this);
        }
        logID = ge.initLog("WaAS");
        hh = new WaAS_HHOLD_Handler(this, files.getInputDataDir());
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
     * Attempts to swap some WaAS data.
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
     * Attempts to swap some WaAS data.
     *
     * @return {@code true} iff some data was successfully swapped.
     */
    @Override
    public boolean swapDataAny() {
        boolean r;
        r = clearSomeData();
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
     * Attempts to clear some data from {@link #data}.
     * @return {@code true} iff some data was successfully cleared.
     */
    public boolean clearSomeData() {
        return data.clearSomeData();
    }

    /**
     * Attempts to clear all data from {@link #data}.
     * @return The amount of data successfully cleared.
     */
    public int clearAllData() {
        int r;
        r = data.clearAllData();
        return r;
    }
    
    /**
     * Serialises and writes {@link data} to {@link #files.getEnvDataFile}.
     */
    public void cacheData() {
        String m = "cacheData";
        logStartTag(m);
        File f = files.getEnvDataFile();
        Generic_IO.writeObject(data, f);
        logEndTag(m);
    }

    /**
     * Loads {@link data} from {@link #files.getEnvDataFile}.
     */
    public final void loadData() {
        String m = "loadData";
        logStartTag(m);
        File f = files.getEnvDataFile();
        data = (WaAS_Data) Generic_IO.readObject(f);
        logEndTag(m);
    }
}
