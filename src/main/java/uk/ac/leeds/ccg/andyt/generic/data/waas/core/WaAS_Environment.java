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
import java.io.IOException;
import uk.ac.leeds.ccg.andyt.data.core.Data_Environment;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.handlers.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;

/**
 *
 * @author geoagdt
 */
public class WaAS_Environment extends WaAS_MemoryManager {

    public transient final Data_Environment de;
    public transient final WaAS_Files files;
    public transient final WaAS_HHOLD_Handler hh;
    public transient WaAS_Data data;
    
    /**
     * For convenience.
     */
    public transient final Generic_Environment env;
    public transient final Generic_IO io;
    
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

    public WaAS_Environment() throws IOException {
        this(new Data_Environment());
    }
    
    public WaAS_Environment(Data_Environment e) throws IOException {
        this(e, e.files.getDir());
    }
    
    public WaAS_Environment(File dataDir) throws IOException {
        this(new Data_Environment(), dataDir);
    }
    
    public WaAS_Environment(Data_Environment e, File dataDir) throws IOException {
        de = e;
        env = e.env;
        io = env.io;
        files = new WaAS_Files(dataDir);
        File f = files.getEnvDataFile();
        if (f.exists()) {
            data = (WaAS_Data) io.readObject(f);
            initData();
            //data.we = this;
        } else {
            data = new WaAS_Data(this);
        }
        logID = env.initLog(WaAS_Strings.s_WaAS);
        hh = new WaAS_HHOLD_Handler(this);
        Memory_Threshold = 2000000000L;
    }

    private void initData() {
        data.we = this;
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
            if (!cacheDataAny()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to cache some of {@link #data}.
     *
     * @param hoome handleOutOfMemoryError
     * @return {@code true} iff some data was successfully cacheped.
     */
    @Override
    public boolean cacheDataAny(boolean hoome) {
        try {
            boolean r = cacheDataAny();
            checkAndMaybeFreeMemory();
            return r;
        } catch (OutOfMemoryError e) {
            if (hoome) {
                clearMemoryReserve();
                boolean r = cacheDataAny(HOOMEF);
                initMemoryReserve();
                return r;
            } else {
                throw e;
            }
        }
    }

    /**
     * Attempts to cache some of {@link #data}.
     *
     * @return {@code true} iff some data was successfully cacheped.
     */
    @Override
    public boolean cacheDataAny() {
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
        env.logStartTag(m);
        io.writeObject(data, files.getEnvDataFile());
        env.logEndTag(m);
    }

    /**
     * 
     * @param s The tag name.
     */
    public final void logStartTagMem(String s) {
        env.logStartTag(s, logID);
        env.log("TotalFreeMemory " + getTotalFreeMemory());
    }

    public final void logEndTagMem(String s) {
        env.log("TotalFreeMemory " + getTotalFreeMemory());
        env.logEndTag(s, logID);
    }
    
}
