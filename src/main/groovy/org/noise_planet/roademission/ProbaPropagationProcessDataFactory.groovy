package org.noise_planet.roademission

import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

class ProbaPropagationProcessDataFactory implements PointNoiseMap.PropagationProcessDataFactory {

    @Override
    PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new ProbaPropagationProcessData(freeFieldFinder)
    }
}
