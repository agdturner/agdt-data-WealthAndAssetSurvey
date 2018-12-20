package uk.ac.leeds.ccg.andyt.generic.data.waas.core;

import java.io.File;
import java.io.Serializable;
import uk.ac.leeds.ccg.andyt.generic.core.Generic_Environment;
//import uk.ac.leeds.ccg.andyt.data.postcode.Generic_UKPostcode_Handler;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.WaAS_Data;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_Environment extends WaAS_OutOfMemoryErrorHandler
        implements Serializable {

    public transient Generic_Environment ge;
    public transient WaAS_Strings Strings;
    public transient WaAS_Files Files;
    
    /**
     * Data.
     */
    public WaAS_Data data;

    public transient static final String EOL = System.getProperty("line.separator");

    public WaAS_Environment() {
        //Memory_Threshold = 3000000000L;
        Strings = new WaAS_Strings();
        Files = new WaAS_Files(Strings, Strings.s_data);
        ge = new Generic_Environment(Files, Strings);
        File f;
        f = Files.getEnvDataFile();
        if (f.exists()) {
            loadData();
            data.Files = Files;
            data.Files.Strings = Strings;
            data.Strings = Strings;
        } else {
            data = new WaAS_Data(Files, Strings);
        }
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

    @Override
    public boolean swapDataAny(boolean handleOutOfMemoryError) {
        try {
            boolean result = swapDataAny();
            checkAndMaybeFreeMemory();
            return result;
        } catch (OutOfMemoryError e) {
            if (handleOutOfMemoryError) {
                clearMemoryReserve();
                boolean result = swapDataAny(HOOMEF);
                initMemoryReserve();
                return result;
            } else {
                throw e;
            }
        }
    }

    /**
     * Currently this just tries to swap WaAS data.
     *
     * @return
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

    public boolean clearSomeData() {
        return data.clearSomeData();
    }

    public int clearAllData() {
        int r;
        r = data.clearAllData();
        return r;
    }
    
    public void cacheData() {
        File f;
        f = Files.getEnvDataFile();
        System.out.println("<cache data>");
        Generic_IO.writeObject(data, f);
        System.out.println("</cache data>");
    }

    public final void loadData() {
        File f;
        f = Files.getEnvDataFile();
        System.out.println("<load data>");
        data = (WaAS_Data) Generic_IO.readObject(f);
        System.out.println("<load data>");
    }
}
