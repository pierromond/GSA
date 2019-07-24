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
class SensitivityProcessData {
    static public Map<Integer, double[]> soundSourceLevels = new HashMap<>()

    protected  Map<Integer, List<double[]>>  wjSourcesD = new HashMap<>()
    protected  Map<Integer, List<double[]>>  wjSourcesE = new HashMap<>()
    protected  Map<Integer, List<double[]>> wjSourcesN = new HashMap<>()

    public static PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()

    // Init des variables
    ArrayList<Integer> Simu = new ArrayList<Integer>() // numero simu
    ArrayList<Double> CalcTime = new ArrayList<Double>() // temps calcul


    ArrayList<Double> TMJA = new ArrayList<Double>()
    ArrayList<Double> PL = new ArrayList<Double>()
    ArrayList<Double> Speed = new ArrayList<Double>()
    ArrayList<Double> Temp = new ArrayList<Double>()
    ArrayList<Integer> Road = new ArrayList<Integer>()

    ArrayList<Double> Dist = new ArrayList<Double>()

    Map<Integer, List<double[]>> getTrafficLevel(String tablename, Sql sql, int nSimu) throws SQLException {
        /**
         * Vehicles category Table 3 P.31 CNOSSOS_EU_JRC_REFERENCE_REPORT
         * lv : Passenger cars, delivery vans ≤ 3.5 tons, SUVs , MPVs including trailers and caravans
         * mv: Medium heavy vehicles, delivery vans > 3.5 tons,  buses, touring cars, etc. with two axles and twin tyre mounting on rear axle
         * hgv: Heavy duty vehicles, touring cars, buses, with three or more axles
         * wav:  mopeds, tricycles or quads ≤ 50 cc
         * wbv:  motorcycles, tricycles or quads > 50 cc
         * @param lv_speed Average light vehicle speed
         * @param mv_speed Average medium vehicle speed
         * @param hgv_speed Average heavy goods vehicle speed
         * @param wav_speed Average light 2 wheels vehicle speed
         * @param wbv_speed Average heavy 2 wheels vehicle speed
         * @param lvPerHour Average light vehicle per hour
         * @param mvPerHour Average heavy vehicle per hour
         * @param hgvPerHour Average heavy vehicle per hour
         * @param wavPerHour Average heavy vehicle per hour
         * @param wbvPerHour Average heavy vehicle per hour
         * @param FreqParam Studied Frequency
         * @param Temperature Temperature (Celsius)
         * @param roadSurface roadSurface empty default, NL01 FR01 ..
         * @param Ts_stud A limited period Ts (in months) over the year where a average proportion pm of light vehicles are equipped with studded tyres and during .
         * @param Pm_stud Average proportion of vehicles equipped with studded tyres
         * @param Junc_dist Distance to junction
         * @param Junc_type Type of junction ((k = 1 for a crossing with traffic lights ; k = 2 for a roundabout)
         */
        //connect
        Map<Integer, List<double[]>> sourceLevel = new HashMap<>()

        // memes valeurs d e et n
        sql.eachRow('SELECT osm_id, the_geom,\n' +
                'LV_SPEE,PV_SPEE,\n' +
                'TMJA_D,TMJA_D,TMJA_D,\n' +
                'PL_D,PL_D,PL_D,PVMT FROM ' + tablename + ';') { row ->

            def list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]

             int id = (int) row[0].toInteger()
            //System.out.println("Source :" + id)
            Geometry the_geom = row[1]
            def lv_d_speed = (double) row[2]
            def mv_d_speed = (double) 0.0
            def hv_d_speed = (double) row[3]
            def wav_d_speed = (double) 0.0
            def wbv_d_speed = (double) 0.0
            def lv_e_speed = (double) row[2]
            def mv_e_speed = (double) 0.0
            def hv_e_speed = (double) row[3]
            def wav_e_speed = (double) 0.0
            def wbv_e_speed = (double) 0.0
            def lv_n_speed = (double) row[2]
            def mv_n_speed = (double) 0.0
            def hv_n_speed = (double) row[3]
            def wav_n_speed = (double) 0.0
            def wbv_n_speed = (double) 0.0
            def TMJAD = (double)  row[4]
            def TMJAE = (double)  row[5]
            def TMJAN =(double)  row[6]
            def PLD =(double)  row[7]
            def PLE =(double)  row[8]
            def PLN =(double)  row[9]
            def vl_d_per_hour = (double) (TMJAD - (PLD*TMJAD/100))
            def ml_d_per_hour = (double) 0.0
            def pl_d_per_hour = (double) (TMJAD*PLD/100)
            def wa_d_per_hour = (double) 0.0
            def wb_d_per_hour = (double) 0.0
            def vl_e_per_hour = (double) (TMJAE- (TMJAE*PLE/100))
            def ml_e_per_hour = (double) 0.0
            def pl_e_per_hour = (double) (TMJAE*PLE/100)
            def wa_e_per_hour = (double) 0.0
            def wb_e_per_hour = (double) 0.0
            def vl_n_per_hour = (double) (TMJAN- (TMJAN*PLN/100))
            def ml_n_per_hour = (double) 0.0
            def pl_n_per_hour = (double) (TMJAN*PLN/100)
            def wa_n_per_hour = (double) 0.0
            def wb_n_per_hour = (double) 0.0
            def Zstart = (double) 0.0
            def Zend = (double) 0.0
            def Juncdist = (double) 250.0
            def Junc_type = (int) 1
            def road_pav = (String) row[10]

            // Ici on calcule les valeurs d'emission par tronçons et par fréquence

            List<double[]> sourceLevel2 = new ArrayList<>()
            List<double[]> sl_res_d = new ArrayList<>()
            List<double[]> sl_res_e = new ArrayList<>()
            List<double[]> sl_res_n = new ArrayList<>()

            for (int r = 0; r < nSimu; ++r) {

                int kk=0
                double[] res_d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                double[] res_e = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                double[] res_n = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

                for (f in list) {
                    // fois 0.5 car moitié dans un sens et moitié dans l'autre
                    /*String RS = "NL01
                    "
                    switch (R_Road[r]){
                        case 1:
                            RS ="NL01"
                            break
                        case 2 :
                            RS ="NL02"
                            break
                        case 3 :
                            RS ="NL03"
                            break
                        case 4 :
                            RS ="NL04"
                            break
                    }*/
                    String RS = "NL05"
                    if (Road ==1){
                        RS = road_pav
                    }
                    RSParametersCnossos srcParameters_d = new RSParametersCnossos(lv_d_speed * Speed[r], mv_d_speed * Speed[r], hv_d_speed * Speed[r], wav_d_speed * Speed[r], wbv_d_speed * Speed[r],
                            vl_d_per_hour * TMJA[r] * 0.5, ml_d_per_hour * TMJA[r] * 0.5, pl_d_per_hour * TMJA[r] * 0.5, wa_d_per_hour * TMJA[r] * 0.5, wb_d_per_hour * TMJA[r] * 0.5,
                            f, Temp[r], RS, 0, 0, 250, 1)
                     RSParametersCnossos srcParameters_e = new RSParametersCnossos(lv_e_speed * Speed[r], mv_e_speed * Speed[r], hv_e_speed * Speed[r], wav_e_speed * Speed[r], wbv_e_speed * Speed[r],
                        vl_e_per_hour * TMJA[r] * 0.5, ml_e_per_hour * TMJA[r] * 0.5, pl_e_per_hour * TMJA[r] * 0.5, wa_e_per_hour * TMJA[r] * 0.5, wb_e_per_hour * TMJA[r] * 0.5,
                        f, Temp[r], RS, 0, 0, 250, 1)
                RSParametersCnossos srcParameters_n = new RSParametersCnossos(lv_n_speed * Speed[r], mv_n_speed * Speed[r], hv_n_speed * Speed[r], wav_n_speed * Speed[r], wbv_n_speed * Speed[r],
                        vl_n_per_hour * TMJA[r] * 0.5, ml_n_per_hour * TMJA[r] * 0.5, pl_n_per_hour * TMJA[r] * 0.5, wa_n_per_hour * TMJA[r] * 0.5, wb_n_per_hour * TMJA[r] * 0.5,
                        f, Temp[r], RS, 0, 0, 250, 1)

                    srcParameters_d.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    srcParameters_e.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    srcParameters_n.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    //res_d[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_d)
                    //res_e[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_e)
                    //res_n[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_n)
                    //srcParameters_d.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    //srcParameters_e.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    //srcParameters_n.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    res_d[kk] =  10 * Math.log10(
                            (1.0/24.0)*
                                    (12*Math.pow(10,(10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_d) / 10) ))/10)
                                    +4* Math.pow(10,(10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_e) / 10) ))/10)
                                    +8* Math.pow(10,(10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_n) / 10) ))/10))
                            )
                    res_d[kk] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_d))
                    res_e[kk] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_e))
                    res_n[kk] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_n))

                    kk++
                }
                sl_res_d.add(res_d)
                sl_res_e.add(res_e)
                sl_res_n.add(res_n)
                sourceLevel2.add(res_d)
            }
            wjSourcesD.put(id,sl_res_d)
            wjSourcesE.put(id,sl_res_e)
            wjSourcesN.put(id,sl_res_n)
            sourceLevel.put(id, sourceLevel2)
        }
    return sourceLevel
}

PropagationProcessPathData getGenericMeteoData(int r) {
    genericMeteoData.setHumidity(70)
    genericMeteoData.setTemperature(Temp[r])
    return genericMeteoData
}


void setSensitivityTable(File file) {
    //////////////////////
    // Import file text
    //////////////////////
    int i_read = 0;
    // Remplissage des variables avec le contenu du fichier plan d'exp
    file.splitEachLine(",") { fields ->

        TMJA.add(fields[0].toFloat())
        PL.add(fields[1].toFloat())
        Speed.add(fields[2].toFloat())
        Temp.add(fields[3].toFloat())
        Road.add(fields[4].toInteger())
        Dist.add(fields[5].toFloat())

        Simu.add(fields[6].toInteger())

        i_read = i_read + 1
    }

}

}
