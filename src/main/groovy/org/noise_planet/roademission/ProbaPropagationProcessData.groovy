package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.h2gis.utilities.SpatialResultSet
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.sql.SQLException

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
@CompileStatic
class ProbaPropagationProcessData extends PropagationProcessData {

    protected List<double[]> wjSourcesD = new ArrayList<>()

    public ProbaPropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder)
    }

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        Geometry the_geom = rs.getGeometry("the_geom")

        double db_m63 = 0
        double db_m125 = 0
        double db_m250 = 0
        double db_m500 =0
        double db_m1000 = 0
        double db_m2000 =0
        double db_m4000 = 0
        double db_m8000 = 0
        int id = rs.getInt("PK")

        double[] res_d =  [db_m63,db_m125,db_m250,db_m500,db_m1000,db_m2000,db_m4000,db_m8000]
        wjSourcesD.add(ComputeRays.dbaToW(res_d))
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }


}
