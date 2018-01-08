package org.rakam.collection.mapper.geoip.maxmind.ip2location;

public class Coordination {
    public final double latitude;
    public final double longitude;

    private Coordination(double lat, double lon) {
        this.latitude = lat;
        this.longitude = lon;
    }

    public static Coordination of(double lat, double lon) {
        return new Coordination(lat, lon);
    }

    @Override
    public String toString() {
        return "Coordination{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Coordination that = (Coordination) o;

        if (Double.compare(that.latitude, latitude) != 0) {
            return false;
        }
        return Double.compare(that.longitude, longitude) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(latitude);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}