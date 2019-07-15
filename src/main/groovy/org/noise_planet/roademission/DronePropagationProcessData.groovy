package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.h2gis.utilities.SpatialResultSet
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.sql.SQLException

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
@CompileStatic
class DronePropagationProcessData extends PropagationProcessData {

    protected List<double[]> wjSourcesD = new ArrayList<>()

    public DronePropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder)
    }

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        Geometry the_geom = rs.getGeometry("the_geom")
        double db_m63 = rs.getDouble("db_m63")
        double db_m125 = rs.getDouble("db_m125")
        double db_m250 = rs.getDouble("db_m250")
        double db_m500 = rs.getDouble("db_m500")
        double db_m1000 = rs.getDouble("db_m1000")
        double db_m2000 = rs.getDouble("db_m2000")
        double db_m4000 = rs.getDouble("db_m4000")
        double db_m8000 = rs.getDouble("db_m8000")
        int t = rs.getInt("T")
        int id = rs.getInt("ID")

        double[] res_d = new double[PropagationProcessPathData.freq_lvl.size()]

        res_d = [db_m63,db_m125,db_m250,db_m500,db_m1000,db_m2000,db_m4000,db_m8000]

        wjSourcesD.add(ComputeRays.dbaToW(res_d))
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }


}
