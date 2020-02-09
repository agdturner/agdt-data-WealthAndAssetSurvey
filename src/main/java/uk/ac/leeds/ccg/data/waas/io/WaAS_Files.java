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
package uk.ac.leeds.ccg.data.waas.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import uk.ac.leeds.ccg.data.io.Data_Files;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;

/**
 * WaAS_Files
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_Files extends Data_Files {

    /**
     * @param dataDir
     * @throws java.io.IOException If encountered.
     */
    public WaAS_Files(Path dataDir) throws IOException {
        super(dataDir);
    }

    public Path getInputWaASDir() throws IOException {
        Path r = Paths.get(getInputDir().toString(), WaAS_Strings.s_WaAS
                , "UKDA-7215-tab", "tab");
        return r;
    }

    public Path getGeneratedWaASDir() throws IOException {
        Path r  = Paths.get(getGeneratedDir().toString(), WaAS_Strings.s_WaAS);
        Files.createDirectories(r);
        return r;
    }
    
    public Path getGeneratedWaASSubsetsDir() throws IOException {
        Path r = Paths.get(getGeneratedWaASDir().toString(), 
                WaAS_Strings.s_Subset);
        Files.createDirectories(r);
        return r;
    }
    
    /**
     * @param wave the wave for which the source input Path is returned.
     * @param type
     * @return the source input Path for a particular WaAS Wave.
     * @throws java.io.IOException If encountered.
     */
    public Path getInputFile(byte wave, String type) throws IOException {
        String filename = "was_wave_" + wave + "_" + type + "_eul_final";
        //if (wave < 4) { // Change for new
        if (wave == 1) {
            filename += "_v2";
        }
        filename += ".tab";
        return Paths.get(getInputWaASDir().toString(), filename);
    }
}
