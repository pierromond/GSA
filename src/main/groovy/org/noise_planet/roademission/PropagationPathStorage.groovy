package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.GeoJSONDocument
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.util.concurrent.ConcurrentLinkedDeque

@CompileStatic
/**
 * Collect path computed by ComputeRays and store it into provided queue (with consecutive receiverId)
 * remove receiverpath or put to keep rays or not
 */
class PropagationPathStorage extends ComputeRaysOut {
    // Thread safe queue object
    protected TrafficPropagationProcessData inputData
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue

    PropagationPathStorage(PropagationProcessData inputData, PropagationProcessPathData pathData, ConcurrentLinkedDeque<PointToPointPaths> pathQueue) {
        super(true, pathData, inputData)
        this.inputData = (TrafficPropagationProcessData)inputData
        this.pathQueue = pathQueue
    }

    @Override
    double[] addPropagationPaths(long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
        return new double[0]
    }

    @Override
    double[] computeAttenuation(PropagationProcessPathData pathData, long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
        /*if (receiverId==11 && sourceId == 42171){
            receiverId == 11
        }*/
        double[] attenuation = super.computeAttenuation(pathData, sourceId, sourceLi, receiverId, propagationPath)
        double[] soundLevelDay = ComputeRays.wToDba(ComputeRays.multArray(inputData.wjSourcesD.get((int) sourceId), ComputeRays.dbaToW(attenuation)))
        double[] soundLevelEve = ComputeRays.wToDba(ComputeRays.multArray(inputData.wjSourcesE.get((int) sourceId), ComputeRays.dbaToW(attenuation)))
        double[] soundLevelNig = ComputeRays.wToDba(ComputeRays.multArray(inputData.wjSourcesN.get((int) sourceId), ComputeRays.dbaToW(attenuation)))
        double[] lDen = new double[soundLevelDay.length]
        double[] lN = new double[soundLevelDay.length]

        /*double[] soundLevelDay = ComputeRays.wToDba(ComputeRays.multArray(inputData.wjSourcesD.get((int)sourceId), ComputeRays.dbaToW(attenuation)))
        double[] l = new double[soundLevelDay.length]
        for(int i = 0; i < soundLevelDay.length; ++i) {
            l[i] = 10.0D*Math.log10( (24.0D/24.0D)*Math.pow(10.0D, soundLevelDay[i]/10.0D))
        }*/
        for (int i = 0; i < soundLevelDay.length; ++i) {
            lDen[i] = 10.0D * Math.log10((12.0D / 24.0D) * Math.pow(10.0D, soundLevelDay[i] / 10.0D)
                    + (4.0D / 24.0D) * Math.pow(10.0D, (soundLevelEve[i] + 5.0D) / 10.0D)
                    + (8.0D / 24.0D) * Math.pow(10.0D, (soundLevelNig[i] + 10.0D) / 10.0D))
            lN[i] = soundLevelNig[i]
        }
        return lDen
    }

    @Override
    void finalizeReceiver(long l) {

    }

    @Override
    IComputeRaysOut subProcess(int i, int i1) {
        return new PropagationPathStorageThread(this)
    }

    static class PropagationPathStorageThread implements IComputeRaysOut {
        // In order to keep consecutive receivers into the deque an intermediate list is built for each thread
        private List<PointToPointPaths> receiverPaths = new ArrayList<>()
        private PropagationPathStorage propagationPathStorage

        PropagationPathStorageThread(PropagationPathStorage propagationPathStorage) {
            this.propagationPathStorage = propagationPathStorage
        }

        @Override
        double[] addPropagationPaths(long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
            PointToPointPaths paths = new PointToPointPaths()
            paths.li = sourceLi
            paths.receiverId = (propagationPathStorage.inputData.receiversPk.get((int) receiverId).intValue())
            paths.sourceId = propagationPathStorage.inputData.sourcesPk.get((int) sourceId).intValue()
            paths.propagationPathList = new ArrayList<>(propagationPath.size())
            for (PropagationPath path : propagationPath) {
                // Copy path content in order to keep original ids for other method calls
                PropagationPath pathPk = new PropagationPath(path.isFavorable(), path.getPointList(),
                        path.getSegmentList(), path.getSRList())
                pathPk.setIdReceiver((int)paths.receiverId)
                pathPk.setIdSource((int)paths.sourceId)
                paths.propagationPathList.add(pathPk)
                receiverPaths.add(paths)
            }
            double[] aGlobalMeteo = propagationPathStorage.computeAttenuation(propagationPathStorage.genericMeteoData, sourceId, sourceLi, receiverId, propagationPath);
            if (aGlobalMeteo != null && aGlobalMeteo.length > 0)  {

                propagationPathStorage.receiversAttenuationLevels.add(new ComputeRaysOut.verticeSL(paths.receiverId, paths.sourceId, aGlobalMeteo))
                return aGlobalMeteo
            } else {
                return new double[0]
            }
        }



        @Override
        void finalizeReceiver(long receiverId) {
            propagationPathStorage.pathQueue.addAll(receiverPaths)
            receiverPaths.clear()
        }

        @Override
        IComputeRaysOut subProcess(int receiverStart, int receiverEnd) {
            return null
        }


    }

}

