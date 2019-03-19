/*
 * Copyright 2018 geoagdt.
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
package uk.ac.leeds.ccg.andyt.generic.data.waas.io;

import java.io.File;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;

/**
 *
 * @author geoagdt
 */
public class WaAS_Files extends Generic_Files {

    public static String DOT_DAT = WaAS_Strings.symbol_dot + WaAS_Strings.s_dat;
    /**
     *
     * @param dataDir
     */
    public WaAS_Files(File dataDir) {
        super(dataDir);
    }

    public File getInputWaASDir() {
        File r = new File(getInputDataDir(), WaAS_Strings.s_WaAS);
        r = new File(r, "UKDA-7215-tab");
        r = new File(r, "tab");
        return r;
    }

    public File getGeneratedWaASDir() {
        File f  = new File(getGeneratedDataDir(), WaAS_Strings.s_WaAS);
        f.mkdirs();
        return f;
    }
    
    public File getGeneratedWaASSubsetsDir() {
        File f = new File(getGeneratedWaASDir(), WaAS_Strings.s_Subsets);
        f.mkdirs();
        return f;
    }

    public File getEnvDataFile() {
        return new File(getGeneratedDataDir(), "Env.dat");
    }
}
