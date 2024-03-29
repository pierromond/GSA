package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.noise_planet.noisemodelling.propagation.PropagationPath

@CompileStatic
class PointToPointPaths {
    ArrayList<PropagationPath> propagationPathList;
    double li
    long sourceId
    long receiverId

    /**
     * Writes the content of this object into <code>out</code>.
     * @param out the stream to write into
     * @throws java.io.IOException if an I/O-error occurs
     */
    void writePropagationPathListStream( DataOutputStream out) throws IOException {

        out.writeLong(receiverId)
        out.writeLong(sourceId)
        out.writeDouble(li)
        out.writeInt(propagationPathList.size())
        for(PropagationPath propagationPath : propagationPathList) {
            propagationPath.writeStream(out);
        }
    }

    /**
     * Reads the content of this object from <code>out</code>. All
     * properties should be set to their default value or to the value read
     * from the stream.
     * @param in the stream to read
     * @throws IOException if an I/O-error occurs
     */
    void readPropagationPathListStream( DataInputStream inputStream) throws IOException {
        if (propagationPathList==null){
            propagationPathList = new ArrayList<>()
        }

        receiverId = inputStream.readLong()
        sourceId = inputStream.readLong()
        li = inputStream.readDouble()
        int propagationPathsListSize = inputStream.readInt()
        propagationPathList.ensureCapacity(propagationPathsListSize)
        for(int i=0; i < propagationPathsListSize; i++) {
            PropagationPath propagationPath = new PropagationPath()
            propagationPath.readStream(inputStream)
            propagationPathList.add(propagationPath)
        }
    }

}
