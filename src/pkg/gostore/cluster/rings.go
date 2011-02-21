// Author: Andre-Philippe Paquet
// Date: November 2010

package cluster

import (
	"gostore"
	"gostore/log"
)

// Collection of cluster rings
type Rings struct {
	rings []*Ring
	count uint8

	globalRing uint8
}

// Create rings from config
func newRingsConfig(ringConfigs []gostore.ConfigRing, globalRing byte) *Rings {
	rings := newRings()
	rings.globalRing = globalRing

	for _, confring := range ringConfigs {
		rings.AddRing(confring.Id, NewRing(int(confring.ReplicationFactor)))
	}

	if rings.GetRing(globalRing) == nil {
		log.Fatal("Specified global ring doesn't exist!!")
	}

	return rings
}

// Returns a new collection of cluster ring
func newRings() *Rings {
	r := new(Rings)
	r.rings = make([]*Ring, 0)
	return r
}

// Adds a ring to the collection
func (r *Rings) AddRing(index uint8, ring *Ring) {
	if index >= uint8(len(r.rings)) {
		newRings := make([]*Ring, index+1)
		copy(newRings, r.rings)
		r.rings = newRings
	}

	r.rings[index] = ring
	r.count++
}

// Returns the ring at the given index
func (r *Rings) GetRing(index uint8) *Ring {
	if index >= uint8(len(r.rings)) {
		return nil
	}

	return r.rings[index]
}

// Returns the global ring
func (r *Rings) GetGlobalRing() *Ring {
	return r.rings[r.globalRing]
}
