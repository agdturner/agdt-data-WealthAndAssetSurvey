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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data.id;

import java.io.Serializable;

/**
 *
 * @author geoagdt
 */
public abstract class WaAS_ID implements Comparable, Serializable {

    protected short ID;

    protected WaAS_ID(short id) {
        this.ID = id;
    }

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return -4;
        } else {
            if (o instanceof WaAS_ID) {
                short oID = ((WaAS_ID) o).ID;
                if (ID > oID) {
                    return 1;
                } else {
                    if (ID < oID) {
                        return -1;
                    }
                    return 0;
                }
            }
            return -3;
        }
    }

    /**
     * @return the ID
     */
    public short getID() {
        return ID;
    }

    @Override
    public String toString() {
        return "ID " + ID;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (o instanceof WaAS_ID) {
            if (ID == ((WaAS_ID) o).ID) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + this.ID;
        return hash;
    }

}
