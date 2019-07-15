package org.noise_planet.roademission

import groovy.sql.Sql
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.sql.SQLException

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
class DynamicProcessData {

    double[] getDroneLevel(String tablename, Sql sql, int t, int idSource) throws SQLException {
        double[] res_d = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        // memes valeurs d e et n
        sql.eachRow('SELECT id, the_geom,\n' +
                'db_m63,db_m125,db_m250,db_m500,db_m1000,db_m2000,db_m4000,db_m8000,t FROM ' + tablename +' WHERE ID = '+ idSource.toString()+' AND T = '+ t.toString()+';') { row ->
            int id = (int) row[0]
            //System.out.println("Source :" + id)
            Geometry the_geom = row[1]
            def db_m63 = row[2]
            def db_m125 = row[3]
            def db_m250 = row[4]
            def db_m500 = row[5]
            def db_m1000 = row[6]
            def db_m2000 = row[7]
            def db_m4000 = row[8]
            def db_m8000 = row[9]
            int time = (int) row[10]


            res_d = [db_m63,db_m125,db_m250,db_m500,db_m1000,db_m2000,db_m4000,db_m8000]

        }

    return res_d
}

}
