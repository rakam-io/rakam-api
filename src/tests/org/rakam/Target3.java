package org.rakam;

/**
* Created by buremba <Burak Emre Kabakcı> on 11/02/15 13:49.
*/
public class Target3 {

    private int id;

    Target3(final int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public void id(final int id) {
        this.id = id;
        System.out.println(id);
    }
}
