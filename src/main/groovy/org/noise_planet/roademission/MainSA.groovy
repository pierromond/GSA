package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.table.Table
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.zip.GZIPInputStream

/**
 * To run
 * Just type "./gradlew runScriptSA -Pworkdir=out2/"
 */

@CompileStatic
class MainSA {
    public static SensitivityProcessData sensitivityProcessData = new SensitivityProcessData()

    static void main(String[] args) {
        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(MainSA.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))

        // Create spatial database
        //TimeZone tz = TimeZone.getTimeZone("UTC")
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault())

        //df.setTimeZone(tz)
        String dbName = new File(workingDir + "database2").toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        File dest2 = new File("data/Exp_comp1m.csv")

        int nvar = 0 // pas toucher
        int nr = 0 // pas toucher
        int nSimu = 0 // ne pas toucher
        int n_comp = 0 // pas toucher
        int i_read = 1   //nombre de lignes d'entête

        // lire les 4 premieres lignes de config
        new File("data/Config.csv").splitEachLine(",") {
            fields ->
                switch (i_read) {
                    case 1:
                        nvar = fields[0].toInteger()
                    case 2:
                        nr = fields[0].toInteger()
                    case 3:
                        nSimu = fields[0].toInteger()
                    case 4:
                        n_comp = fields[0].toInteger()
                    default: break
                }
                i_read = i_read + 1
        }

        // Evaluate receiver points using provided buildings
        logger.info("Read Sources")
        sql.execute("DROP TABLE IF EXISTS RECEIVERS")
        SHPRead.readShape(connection, "data/RecepteursQuest.shp", "RECEIVERS")

        HashMap<Integer,Double> pop = new HashMap<>()
        // memes valeurs d e et n
        sql.eachRow('SELECT id, pop FROM RECEIVERS;') { row ->
            int id = (int) row[0]
            pop.put(id, (Double) row[1])
        }

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/Roads2407.shp", "ROADS2")
        sql.execute("DROP TABLE ROADS if exists;")
        sql.execute('CREATE TABLE ROADS AS SELECT CAST( OSM_ID AS INTEGER ) OSM_ID , ST_UpdateZ(THE_GEOM, 0.05) THE_GEOM, TMJA_D,TMJA_E,TMJA_N,\n' +
                'PL_D,PL_E,PL_N,\n' +
                'LV_SPEE,PV_SPEE, PVMT FROM ROADS2;')
        sql.execute('ALTER TABLE ROADS ALTER COLUMN OSM_ID SET NOT NULL;')
        sql.execute('ALTER TABLE ROADS ADD PRIMARY KEY (OSM_ID);')
        sql.execute("CREATE SPATIAL INDEX ON ROADS(THE_GEOM)")

        logger.info("Road file loaded")

        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()

        PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()
        sensitivityProcessData.setSensitivityTable(dest2)

        int GZIP_CACHE_SIZE = (int) Math.pow(2, 19)

        logger.info("Start time :" + df.format(new Date()))

        try {
            FileInputStream fileInputStream = new FileInputStream(new File("rays2407.gz").getAbsolutePath())
            try {
                GZIPInputStream gzipInputStream = new GZIPInputStream((fileInputStream), GZIP_CACHE_SIZE)
                DataInputStream dataInputStream = new DataInputStream(gzipInputStream)
                System.out.println("Read file and apply sensitivity analysis")
                int oldIdReceiver = -1
                int oldIdSource = -1

                FileWriter csvFile = new FileWriter(new File(workingDir, "simu2.csv"))
                List<double[]> simuSpectrum = new ArrayList<>()

                Map<Integer, List<double[]>>  sourceLevel = new HashMap<>()

                System.out.println("Prepare Sources")
                def timeStart = System.currentTimeMillis()

                sourceLevel = sensitivityProcessData.getTrafficLevel("ROADS2", sql, nSimu)
                def timeStart2 = System.currentTimeMillis()
                System.out.println(timeStart2-timeStart)
                logger.info("End Emission time :" + df.format(new Date()))
                System.out.println("Run SA")
                while (fileInputStream.available() > 0) {

                    PointToPointPaths paths = new PointToPointPaths()
                    paths.readPropagationPathListStream(dataInputStream)
                    int idReceiver = (Integer) paths.receiverId
                    int idSource = (Integer) paths.sourceId

                    if (idReceiver!= oldIdReceiver) {
                        logger.info("Receiver: " + oldIdReceiver)
                        // Save old receiver values
                        if (oldIdReceiver!= -1) {
                            for (int r = 0; r < nSimu; ++r) {
                                csvFile.append(String.format("%d\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\n", r, oldIdReceiver, pop.get(oldIdReceiver), simuSpectrum.get(r)[0], simuSpectrum.get(r)[1], simuSpectrum.get(r)[2], simuSpectrum.get(r)[3], simuSpectrum.get(r)[4], simuSpectrum.get(r)[5], simuSpectrum.get(r)[6], simuSpectrum.get(r)[7]))
                            }
                        }
                        // Create new receiver value
                        simuSpectrum.clear()
                        for (int r = 0; r < nSimu; ++r) {
                            simuSpectrum.add(new double[PropagationProcessPathData.freq_lvl.size()])
                        }
                    }
                    oldIdReceiver = idReceiver
                    ComputeRaysOut out = new ComputeRaysOut(false, sensitivityProcessData.getGenericMeteoData(0))
                    //double[] attenuation = out.computeAttenuation(sensitivityProcessData.getGenericMeteoData(r), idSource, paths.getLi(), idReceiver, propagationPaths)
                    for (int r = 0; r < nSimu; r++) {
                        List<PropagationPath> propagationPaths = new ArrayList<>()

                        for (int pP= 0; pP< paths.propagationPathList.size(); pP++) {
                            paths.propagationPathList.get(pP).initPropagationPath()
                            /*if (
                            paths.propagationPathList.get(pP).refPoints.size() <= sensitivityProcessData.refl[r]
                            && paths.propagationPathList.get(pP).difHPoints.size() <= sensitivityProcessData.dif_H[r]
                            && paths.propagationPathList.get(pP).difVPoints.size() <= sensitivityProcessData.dif_V[r]){*/
                            propagationPaths.add(paths.propagationPathList.get(pP))
                            //}
                        }
                        if (propagationPaths.size()>0) {
                            //double[] attenuation = out.computeAttenuation(sensitivityProcessData.getGenericMeteoData(r), idSource, paths.getLi(), idReceiver, propagationPaths)
                            //double[] soundLevel = sumArray(attenuation, sourceLevel.get(idSource).get(r))

                            double[] attenuation = out.computeAttenuation(sensitivityProcessData.getGenericMeteoData(r), idSource, paths.getLi(), idReceiver, propagationPaths)
                            double[] soundLevelDay = ComputeRays.wToDba(ComputeRays.multArray(sensitivityProcessData.wjSourcesD.get(idSource).get(r), ComputeRays.dbaToW(attenuation)))
                            double[] soundLevelEve = ComputeRays.wToDba(ComputeRays.multArray(sensitivityProcessData.wjSourcesE.get(idSource).get(r), ComputeRays.dbaToW(attenuation)))
                            double[] soundLevelNig = ComputeRays.wToDba(ComputeRays.multArray(sensitivityProcessData.wjSourcesN.get(idSource).get(r), ComputeRays.dbaToW(attenuation)))
                            double[] lDen = new double[soundLevelDay.length]
                            double[] lN = new double[soundLevelDay.length]
                            for(int i = 0; i < soundLevelDay.length; ++i) {
                                lDen[i] = 10.0D*Math.log10( (12.0D/24.0D)*Math.pow(10.0D, soundLevelDay[i]/10.0D)
                                        +(4.0D/24.0D)*Math.pow(10.0D, (soundLevelEve[i]+5.0D)/10.0D)
                                        +(8.0D/24.0D)*Math.pow(10.0D, (soundLevelNig[i]+10.0D)/10.0D))
                                lN[i] = soundLevelNig[i]
                            }

                            simuSpectrum[r] = ComputeRays.sumDbArray(simuSpectrum[r], lDen)
                        }
                    }

                }
                csvFile.close()
                logger.info("End time :" + df.format(new Date()))

            } finally {

                fileInputStream.close()
            }


        }

        finally {

        }

    }


    private static double[] sumArray(double[] array1, double[] array2) {
        if (array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array")
        } else {
            double[] sum = new double[array1.length]

            for (int i = 0; i < array1.length; ++i) {
                sum[i] = array1[i] + array2[i]
            }

            return sum
        }
    }
// fonction pour Copier les fichiers dans un autre répertoire
    private static void copyFileUsingStream(File source, File dest) throws IOException {
        InputStream is = null
        OutputStream os = null
        try {
            is = new FileInputStream(source)
            os = new FileOutputStream(dest)
            byte[] buffer = new byte[1024]
            int length
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length)
            }
        } finally {
            is.close()
            os.close()
        }
    }

}
