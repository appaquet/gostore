package db

import (
	"os"
	"fmt"
	"math"
	"gostore/tools/buffer"
	"gostore/cluster"
	"gostore/tools/typedio"
	"gostore/log"
)

const (
	SEG_CHUNKING = 4
	SEG_MAX_SIZE = 4294967296 // 2**32
	//SEG_MAX_SIZE = 50000
)

// TODO: Thread safe!

type Token uint16

type TokenRange struct {
	from Token
	to   Token
}

func (r *TokenRange) Length() int {
	return int(r.to) - int(r.from)
}

func (r *TokenRange) IsWithin(token Token) bool {
	if token <= r.to && token >= r.from {
		return true
	}

	return false
}


//
// Segments manager of the db
//
type segmentManager struct {
	db *Db

	id2segments   []*segment
	nextSegId     uint16
	timelineStart *segmentCollection
	timelineEnd   *segmentCollection

	dataDir string
	tokens  TokenRange
}

func newSegmentManager(db *Db, dataDir string, token_before, token Token) *segmentManager {
	segManager := &segmentManager{
		db: db,
		id2segments: make([]*segment, cluster.MAX_NODE_ID),
		timelineStart: newSegmentCollection(),
		timelineEnd: newSegmentCollection(),
		dataDir: dataDir,
		tokens: TokenRange{token_before, token},
	}

	// TODO: Scan directory, create all segments
	// TODO: Sort them by start position
	// TODO: Create the timeline


	return segManager
}

func (m *segmentManager) getCurrentSegment(token Token) *segment {
	if !m.tokens.IsWithin(token) {
		log.Fatal("Got a token not within range: got %d, range from %d to %d", token, m.tokens.from, m.tokens.to)
	}

	seg := m.timelineEnd.getSegment(token)

	// no current segment found, create one
	if seg == nil || !seg.current {
		if seg == nil {
			log.Debug("Couldn't find a segment for token %d", token)
		} else {
			log.Debug("Segment for token %d is not current", token)
		}

		// find the right chunk for this token
		chunkLength := int(math.Ceil(float64(m.tokens.Length()) / SEG_CHUNKING))

		found := false
		chunk := m.tokens
		for !found {
			to := int(chunk.from) + chunkLength
			if to > int(m.tokens.to) { // prevent overflow
				to = int(m.tokens.to)
			}
			chunk = TokenRange{chunk.from, Token(to)}

			if chunk.IsWithin(token) {
				found = true
			} else {
				chunk = TokenRange{chunk.to + 1, m.tokens.to}
			}
		}

		pos := uint64(0)
		if seg != nil {
			pos = seg.position_end
		}

		log.Info("Creating a new segment for tokens %d to %d @ %d", chunk.from, chunk.to, pos)
		seg = createSegment(m.dataDir, chunk.from, chunk.to, pos)
		m.timelineEnd.addSegment(seg)

		// asign an id
		// TODO: Make sure we don't collide
		seg.id = m.nextSegId
		m.id2segments[seg.id] = seg
		m.nextSegId++
	}

	return seg
}

func (m *segmentManager) writeMutation(token Token, mutation *mutation) (segment *segment, entry *segmentEntry) {
	segment = m.getCurrentSegment(token)

	entry = &segmentEntry{
		segment: segment,
		token: token,
		mutation: mutation,
	}

	// write the entry
	segment.write(entry)

	// check if the segment can still be written after
	size := segment.position_end - segment.position_start
	if size >= uint64(SEG_MAX_SIZE) {
		segment.current = false
		log.Info("Segment %s too big for a new entry. Rotating!", segment)
	}

	return
}


//
// Collection of segments
//
type segmentCollection struct {
	segments []*segment
}

func newSegmentCollection() *segmentCollection {
	col := new(segmentCollection)
	col.segments = make([]*segment, 0)
	return col
}

func (c *segmentCollection) getSegment(token Token) *segment {
	var candCurrent *segment
	var cand *segment
	for _, seg := range c.segments {
		if seg.tokens.IsWithin(token) {
			if seg.current {
				candCurrent = seg
			}
			cand = seg
		}
	}

	// return a current segment first (last)
	if candCurrent != nil {
		return candCurrent
	}
	return cand
}

func (c *segmentCollection) addSegment(newseg *segment) {
	// unset as current all segments that overlapse with this new segment
	for _, seg := range c.segments {
		if (newseg.tokens.from >= seg.tokens.from && newseg.tokens.from <= seg.tokens.to) ||
			(newseg.tokens.to >= seg.tokens.from && newseg.tokens.to <= seg.tokens.to) {

			log.Debug("New segment %s overlapse %s. Closing overlapsed!", newseg, seg)
			seg.current = false

			// TODO: add a "way" parameter so we add to front or back
			seg.nextSegments.segments = append(seg.nextSegments.segments, newseg)
			newseg.prevSegments.segments = append(newseg.prevSegments.segments, seg)
		}
	}

	c.segments = append(c.segments, newseg)
	c.cleanup()
}


//
// Remove all not current segments that aren't covering any part of the range anymore
//
func (c *segmentCollection) cleanup() {
	log.Debug("Cleaning the segment collections...")
	newSegs := make([]*segment, 0)

	for o := 0; o < len(c.segments); o++ {
		oSeg := c.segments[o]

		if !oSeg.current {
			covRange := oSeg.tokens
			useless := false

			// iterate on all current segments, remove ranges covered from other segments from covRange
			for i := 0; i < len(c.segments) && !useless; i++ {
				iSeg := c.segments[i]

				if i != o && iSeg.tokens.IsWithin(covRange.from) && iSeg.position_start > oSeg.position_start {
					covRange = TokenRange{iSeg.tokens.to, covRange.to}
				}

				// as soon as the coverage range is <= 0, we know that this range is useless
				if covRange.Length() <= 0 {
					useless = true
					log.Debug("Segment %s now useless. Removing it.", oSeg)
				}
			}

			// only add the segment to the new array if it isn't useless
			if !useless {
				newSegs = append(newSegs, oSeg)
			}

		} else {
			// if its a current segment, add it
			newSegs = append(newSegs, oSeg)
		}
	}

	c.segments = newSegs
}


//
// Segment (file) in which entries are written sequentially
//
type segment struct {
	id           uint16
	tokens       TokenRange
	nextSegments *segmentCollection
	prevSegments *segmentCollection

	position_start uint64 // absolute start position
	position_end   uint64 // absolute (exclusive) end of segment (ex: start=0, size=10, end=10)

	fd      *os.File
	typedFd typedio.ReadWriter
	current bool
}

func openSegment(path string) *segment {
	seg := new(segment)
	seg.nextSegments = newSegmentCollection()
	seg.prevSegments = newSegmentCollection()

	var err os.Error

	stat, err := os.Stat(path)
	if stat == nil || err != nil {
		log.Fatal("Couldn't stat segment file %s: %s", path, err)
	}

	var from, to Token
	_, err = fmt.Sscanf(stat.Name, "%04X_%04X_%016X.seg", &from, &to, &seg.position_start)
	if err != nil {
		log.Fatal("Couldn't read segment file name %s: %s", path, err)
	}
	seg.tokens = TokenRange{from, to}

	seg.fd, err = os.Create(path)
	if err != nil {
		log.Fatal("Couldn't open segment %s: %s", path, err)
	}
	seg.typedFd = typedio.NewReadWriter(seg.fd)
	seg.position_end = seg.position_start + uint64(stat.Size)

	return seg
}

func createSegment(dataDir string, token_from, token_to Token, position uint64) *segment {
	seg := new(segment)
	seg.nextSegments = newSegmentCollection()
	seg.prevSegments = newSegmentCollection()
	seg.tokens = TokenRange{token_from, token_to}
	seg.position_start = position
	seg.position_end = seg.position_start

	var err os.Error

	filePath := fmt.Sprintf("%s/%04X_%04X_%016X.seg", dataDir, token_from, token_to, position)
	seg.fd, err = os.Create(filePath)
	if err != nil {
		log.Fatal("Couldn't open segment %s: %s", filePath, err)
	}
	seg.typedFd = typedio.NewReadWriter(seg.fd)
	seg.current = true

	return seg
}

func (s *segment) write(entry *segmentEntry) {
	s.fd.Seek(int64(s.position_end-s.position_start), 0)
	entry.position = s.position_end
	entry.serialize(s.typedFd)
	s.position_end += uint64(entry.totalSize())
}

func (s *segment) String() string {
	size := s.position_end - s.position_start
	return fmt.Sprintf("Segment[from=%d, to=%d, current=%v, pos_start=%d, pos_end=%d, size=%d]", s.tokens.from, s.tokens.to, s.current, s.position_start, s.position_end, size)
}


type segmentEntry struct {
	segment	 *segment

	position uint64
	token    Token
	mutsize  uint32
	mutation *mutation
}

func (e *segmentEntry) serialize(writer typedio.Writer) {
	// TODO: Check for errors
	writer.WriteUint64(e.position) // position

	buf := buffer.New()

	e.mutation.serialize(buf) // TODO: Handle error
	e.mutsize = uint32(buf.Size)

	writer.WriteUint16(uint16(e.token)) // token
	writer.WriteUint32(e.mutsize)       // size of mutation

	buf.Seek(0, 0)
	writer.Write(buf.Bytes()) // mutation
}

func (e *segmentEntry) totalSize() uint32 {
	return 8 + // pos
		2 + // token
		4 + // size
		e.mutsize // mut
}

func (e *segmentEntry) relativePosition() uint32 {
	return uint32(e.position - e.segment.position_start)
}
