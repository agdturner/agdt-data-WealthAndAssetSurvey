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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import java.io.File;
import java.util.HashSet;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.process.WaAS_Main_Process;

/**
 *
 * @author geoagdt
 */
public abstract class WaAS_Handler {

    public transient WaAS_Files Files;
    public transient WaAS_Strings Strings;
    protected String TYPE;
    protected File INDIR;

    public WaAS_Handler(WaAS_Files Files, WaAS_Strings Strings) {
        this.Files = Files;
        this.Strings = Strings;
    }

    public File getInputFile(byte wave) {
        File f;
        String filename;
        filename = "was_wave_" + wave + "_" + TYPE + "_eul_final";
        if (wave < 4) {
            filename += "_v2";
        }
        filename += ".tab";
        f = new File(INDIR, filename);
        return f;
    }

    protected Object load(byte wave, File f) {
        Object r;
        String m;
        m = "load " + getString0(wave, f);
        WaAS_Main_Process.log1("<" + m + ">");
        r = Generic_IO.readObject(f);
        WaAS_Main_Process.log1("</" + m + ">");
        return r;
    }

    protected String getString0(byte wave, File f) {
        return "wave " + wave + " " + TYPE + " WaAS file " + f;
    }

    protected void cache(byte wave, File f, Object o) {
        String m;
        m = "store " + getString0(wave, f);
        WaAS_Main_Process.log1("<" + m + ">");
        Generic_IO.writeObject(o, f);
        WaAS_Main_Process.log1("</" + m + ">");
    }

    public void cacheSubset(byte wave, Object o) {
        File f;
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                TYPE + wave + "." + Strings.S_dat);
        cache(wave, f, o);
    }

    public void cacheSubsetLookups(byte wave, TreeMap<Short, HashSet<Short>> m0, 
            TreeMap<Short, Short> m1) {
        File f;
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                TYPE + wave + "To" + (wave + 1) + "." + Strings.S_dat);        
        cache(wave, f, m0);
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                TYPE + (wave + 1) + "To" + wave + "." + Strings.S_dat);        
        cache(wave, f, m1);
    }
    
    public void cacheSubsetCollection(short cID, byte wave, Object o) {
        File f;
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                getString1(wave, cID) + "." + Strings.S_dat);
        WaAS_Handler.this.cache(wave, f, o);
    }

    protected String getString1(byte wave, short cID) {
        return TYPE + wave + "_" + cID;
    }

    public Object[] loadSubsetLookups(byte wave) {
        Object[] r;
        r = new Object[2];
        TreeMap<Short, HashSet<Short>> m0;
        TreeMap<Short, Short> m1;
        File f;
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                TYPE + wave + "To" + (wave + 1) + "." + Strings.S_dat);        
        m0 = (TreeMap<Short, HashSet<Short>>) Generic_IO.readObject(f);
        r[0] = m0;
        cache(wave, f, m0);
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                TYPE + (wave + 1) + "To" + wave + "." + Strings.S_dat);   
        m1 = (TreeMap<Short, Short>) Generic_IO.readObject(f);
        r[1] = m1;
        return r;
    }
    
    public Object loadSubsetCollection(short cID, byte wave) {
        Object r;
        File f;
        f = new File(Files.getGeneratedWaASSubsetsDir(),
                getString1(wave, cID) + "." + Strings.S_dat);
        r = load(wave, f);
        return r;
    }
}
