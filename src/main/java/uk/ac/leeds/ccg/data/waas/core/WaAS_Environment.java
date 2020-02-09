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
package uk.ac.leeds.ccg.data.waas.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import uk.ac.leeds.ccg.data.core.Data_Environment;
import uk.ac.leeds.ccg.generic.core.Generic_Environment;
import uk.ac.leeds.ccg.generic.core.Generic_Strings;
import uk.ac.leeds.ccg.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.data.waas.data.handlers.WaAS_HHOLD_Handler;
import uk.ac.leeds.ccg.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.generic.io.Generic_Defaults;
import uk.ac.leeds.ccg.generic.io.Generic_IO;
import uk.ac.leeds.ccg.generic.memory.Generic_MemoryManager;

/**
 * WaAS_Environment
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_Environment extends Generic_MemoryManager {

    public transient Data_Environment de;
    public transient WaAS_Files files;
    public transient WaAS_HHOLD_Handler hh;
    public transient WaAS_Data data;

    /**
     * For convenience. 
     */
    public transient Generic_Environment env;
    public transient Generic_IO io;

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

    public WaAS_Environment(Path dataDir) throws Exception {
        this(new Data_Environment(new Generic_Environment(
                new Generic_Defaults(dataDir))), dataDir);
    }

    public WaAS_Environment(Data_Environment e, Path dataDir) throws IOException, Exception {
        /**
         * Init de.
         */
        de = e;
        Path d0 = Paths.get(dataDir.toString(), Generic_Strings.s_generated);
        Path d = Paths.get(d0.toString(), Generic_Strings.s_data);
        de.files.setDir(d);
        de.initLog(Generic_Strings.s_data);
        /**
         * Init env.
         */
        env = e.env;
        d = Paths.get(d0.toString(), Generic_Strings.s_generic);
        env.files.setDir(d);
        env.initLog(Generic_Strings.s_generic);
        /**
         * Init io, files, data, hh and Memory_Threshold
         */
        io = env.io;
        files = new WaAS_Files(dataDir);
        Path f = files.getEnvDataFile();
        if (Files.exists(f)) {
            data = (WaAS_Data) Generic_IO.readObject(f);
            initData();
            //data.we = this;
        } else {
            data = new WaAS_Data(this);
        }
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
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean checkAndMaybeFreeMemory() throws IOException {
        System.gc();
        while (getTotalFreeMemory() < Memory_Threshold) {
//            int clear = clearAllData();
//            if (clear == 0) {
//                return false;
//            }
            if (!swapSomeData()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to cache some of {@link #data}.
     *
     * @param hoome handleOutOfMemoryError
     * @return {@code true} iff some data was successfully cached.
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean swapSomeData(boolean hoome) throws IOException {
        try {
            boolean r = swapSomeData();
            checkAndMaybeFreeMemory();
            return r;
        } catch (OutOfMemoryError e) {
            if (hoome) {
                clearMemoryReserve(env);
                boolean r = WaAS_Environment.this.swapSomeData(HOOMEF);
                initMemoryReserve(env);
                return r;
            } else {
                throw e;
            }
        }
    }

    /**
     * Attempts to cache some of {@link #data}.
     *
     * @return {@code true} if some data was successfully cached.
     * @throws java.io.IOException If encountered.
     */
    @Override
    public boolean swapSomeData() throws IOException {
        boolean r = clearSomeData();
        if (r) {
            return r;
        } else {
            env.log("No WaAS data to clear. Do some coding to try "
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
     * @throws java.io.IOException If encountered.
     */
    public boolean clearSomeData() throws IOException {
        return data.clearSomeData();
    }

    /**
     * Attempts to clear all of {@link #data} using
     * {@link WaAS_Data#clearAllData()}.
     *
     * @return The amount of data successfully cleared.
     * @throws java.io.IOException If encountered.
     */
    public int clearAllData() throws IOException {
        int r = data.clearAllData();
        return r;
    }

    /**
     * For caching {@link #data} to {@link WaAS_Files#getEnvDataFile()}.
     * @throws java.io.IOException If encountered.
     */
    public void swapData() throws IOException {
        String m = "cacheData";
        env.logStartTag(m);
        Generic_IO.writeObject(data, files.getEnvDataFile());
        env.logEndTag(m);
    }

    /**
     * For logging that a line has not loaded.
     * 
     * @param ex
     * @param ln 
     */
    public void logLineNotLoading(Exception ex, int ln) {
        env.log("line " + ln + " could not be loaded");
        env.log(ex.getMessage());
    }
}
