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
import java.io.IOException;
import uk.ac.leeds.ccg.andyt.data.io.Data_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;

/**
 *
 * @author geoagdt
 */
public class WaAS_Files extends Data_Files {

    /**
     * @param dataDir
     */
    public WaAS_Files(File dataDir) throws IOException {
        super(dataDir);
    }

    public File getInputWaASDir() {
        File r = new File(getInputDir(), WaAS_Strings.s_WaAS);
        r = new File(r, "UKDA-7215-tab");
        r = new File(r, "tab");
        return r;
    }

    public File getGeneratedWaASDir() {
        File r  = new File(getGeneratedDir(), WaAS_Strings.s_WaAS);
        r.mkdirs();
        return r;
    }
    
    public File getGeneratedWaASSubsetsDir() {
        File f = new File(getGeneratedWaASDir(), WaAS_Strings.s_Subsets);
        f.mkdirs();
        return f;
    }
    
    /**
     * @param wave the wave for which the source input File is returned.
     * @param type
     * @return the source input File for a particular WaAS Wave.
     */
    public File getInputFile(byte wave, String type) {
        String filename;
        filename = "was_wave_" + wave + "_" + type + "_eul_final";
        //if (wave < 4) { // Change for new
        if (wave == 1) {
            filename += "_v2";
        }
        filename += ".tab";
        return new File(getInputWaASDir(), filename);
    }
}
