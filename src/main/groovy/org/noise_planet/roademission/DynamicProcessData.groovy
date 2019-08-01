package org.noise_planet.roademission

import groovy.sql.Sql
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceDynamic
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.emission.RSParametersDynamic

import java.sql.SQLException

import static org.junit.Assert.assertEquals

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
class DynamicProcessData {

    protected  Map<Integer, Double>  PL = new HashMap<>()
    protected  Map<Integer, Double>  SPEED = new HashMap<>()
    protected  Map<Integer, Double>  TV = new HashMap<>()


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

    double[] getCarsLevel(int t, int idSource) throws SQLException {
        double[] res_d = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        double[] res_TV = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        double[] res_PL = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        def list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]
        // memes valeurs d e et n


            def random = Math.random()
            if (random < TV.get(idSource)){
                int kk=0
                for (f in list) {

                    double speed = SPEED.get(idSource)
                    int acc = 0
                    int FreqParam = f
                    double Temperature = 20
                    int RoadSurface = 0
                    boolean Stud = true
                    double Junc_dist = 200
                    int Junc_type = 1
                    int veh_type = 1
                    int acc_type= 1
                    double LwStd= 1
                    int VehId = 10

                    RSParametersDynamic rsParameters = new RSParametersDynamic(speed,  acc,  veh_type, acc_type, FreqParam,  Temperature,  RoadSurface,Stud, Junc_dist, Junc_type,LwStd,VehId)
                    rsParameters.setSlopePercentage(0)

                    res_TV[kk] = EvaluateRoadSourceDynamic.evaluate(rsParameters)
                    kk++
                }

            }
            if (random < PL.get(idSource)){
                int kk=0
                for (f in list) {
                    double speed = SPEED.get(idSource)
                    int acc = 0
                    int FreqParam = f
                    double Temperature = 20
                    int RoadSurface = 0
                    boolean Stud = true
                    double Junc_dist = 200
                    int Junc_type = 1
                    int veh_type = 3
                    int acc_type= 1
                    double LwStd= 1
                    int VehId = 10

                    RSParametersDynamic rsParameters = new RSParametersDynamic(speed,  acc,  veh_type, acc_type, FreqParam,  Temperature,  RoadSurface,Stud, Junc_dist, Junc_type,LwStd,VehId)
                    rsParameters.setSlopePercentage(0)

                    res_PL[kk] = EvaluateRoadSourceDynamic.evaluate(rsParameters)
                    kk++
                }
            }
            int kk=0
            for (f in list) {
                res_d[kk] = 10 * Math.log10(
                        (1.0 / 2.0) *
                                ( Math.pow(10, (10 * Math.log10(Math.pow(10, res_TV[kk] / 10))) / 10)
                                        + Math.pow(10, (10 * Math.log10(Math.pow(10, res_PL[kk] / 10))) / 10)
                                        )
                )
            kk++
            }




        return res_d
    }



    void setProbaTable(String tablename, Sql sql) {
        //////////////////////
        // Import file text
        //////////////////////
        int i_read = 0;
        // Remplissage des variables avec le contenu du fichier plan d'exp
        sql.eachRow('SELECT PK, SPEED, DENSITY_TV, DENSITY_PL FROM ' + tablename +';') { row ->
            int pk = row[0].toInteger()
            SPEED.put(pk,row[1].toFloat())
            TV.put(pk,row[2].toFloat())
            PL.put(pk,row[3].toFloat())

        }

    }
}
