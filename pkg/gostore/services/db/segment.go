package db

import (
	"os"
	"fmt"
	"math"
	"sort"
	"gostore/tools/buffer"
	"gostore/cluster"
	"gostore/tools/typedio"
	"gostore/log"
	"sync"
)

const (
	SEG_CHUNKING = 4
	SEG_MAX_SIZE = 4294967296 // 2**32
)

// TODO: Segment cleanups
// TODO: Thread safe!

// TODO: Move into cluster
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

func (r *TokenRange) IsOverlapsing(itsRange TokenRange) bool {
	if (itsRange.from >= r.from && itsRange.from <= r.to) || (itsRange.to >= r.from && itsRange.to <= r.to) {
		return true
	}

	return false
}


//
// Segments manager
//
type segmentManager struct {
	segments   []*segment
	nextSegId     uint16
	timeline	*segmentCollection

	dataDir string
	tokens  TokenRange

	segMaxSize uint64
}

func newSegmentManager(dataDir string, tokenBefore, token Token) *segmentManager {
	m := &segmentManager{
		segments: make([]*segment, cluster.MAX_NODE_ID+1),
		timeline: newSegmentCollection(),
		dataDir: dataDir,
		tokens: TokenRange{tokenBefore, token},
		segMaxSize: SEG_MAX_SIZE,
	}

	dir, err := os.Open(dataDir)
	if err != nil {
		panic(fmt.Sprintf("Couldn't open data directory: %s\n", err))
	}

	segFiles, err := dir.Readdirnames(-1)
	if err != nil {
		panic(fmt.Sprintf("Couldn't open data directory: %s\n", err))
	}

	// sort segments by starting position to load them in the right order
	sort.SortStrings(segFiles)

	// load each segment, add them (building the end of the timeline)
	for _, segFile := range segFiles {
		seg := openSegment(dataDir + "/" + segFile)

		seg.id = m.nextSegId
		m.segments[seg.id] = seg
		m.nextSegId++

		m.timeline.addSegment(seg)
	}

	return m
}

func (m *segmentManager) getWritableSegment(token Token) *segment {
	if !m.tokens.IsWithin(token) {
		log.Fatal("Got a token not within range: got %d, range from %d to %d", token, m.tokens.from, m.tokens.to)
	}

	seg := m.timeline.getEndSegment(token)

	// no writable segment found, create one
	if seg == nil || !seg.writable {
		if seg == nil {
			log.Debug("Couldn't find a segment for token %d", token)
		} else {
			log.Debug("Segment for token %d is not writable", token)
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
			pos = seg.positionEnd // TODO: THIS IS NOT GOOD! IT SHOULD TAKE THE BIGGEST END POSITION OF ALL OVERRIDEN SEGMENTS
		}

		log.Info("Creating a new segment for tokens %d to %d @ %d", chunk.from, chunk.to, pos)
		seg = createSegment(m.dataDir, chunk.from, chunk.to, pos)
		m.timeline.addSegment(seg)

		// find an id, assign it to the segment
		for m.segments[m.nextSegId] != nil {
			m.nextSegId++
		}
		seg.id = m.nextSegId
		m.segments[seg.id] = seg
		m.nextSegId++
	}

	return seg
}

func (m *segmentManager) writeExecuteMutation(token Token, mutation *mutation, db *Db) (err os.Error) {
	m.writeMutation(token, mutation)
	err = mutation.execute(db, false) // execute, non-replay
	return
}

func (m *segmentManager) writeMutation(token Token, mutation *mutation) {
	// TODO: Thread safe

	segment := m.getWritableSegment(token)

	// create the entry
	entry := segment.createEntry(token)
	entry.mutation = mutation

	// write the entry
	segment.write(entry)
	mutation.seg = segment
	mutation.segEntry = entry

	// check if the segment can still be written after
	size := segment.positionEnd - segment.positionStart
	if size >= m.segMaxSize {
		segment.writable = false
		log.Info("Segment %s too big for a new entry. Rotating!", segment)
	}

	return
}

func (m *segmentManager) closeAll() {
	for _, seg := range m.segments {
		if seg != nil {
			seg.fd.Close()
		}
	}
}

func (m *segmentManager) replayAll(db *Db) (err os.Error) {
	log.Info("Replaying all segments...")

	for _, seg := range m.segments {
		if seg != nil {
			err = seg.replay(db)
			if err != nil {
				return err
			}
		}
	}

	log.Info("All segments replayed")

	return
}

func (m *segmentManager) getSegment(id uint16) (seg *segment) {
	return m.segments[id]
}


//
// Chained collection of segments ordered by their
// positions in the timeline of segments
//
type segmentCollection struct {
	beginning []*segment
	end       []*segment
}

func newSegmentCollection() *segmentCollection {
	col := new(segmentCollection)
	col.beginning = make([]*segment, 0)
	col.end = make([]*segment, 0)
	return col
}

func (c *segmentCollection) getEndSegment(token Token) *segment {
	return c.getSegment(true, token)
}

func (c *segmentCollection) getBeginningSegment(token Token) *segment {
	return c.getSegment(false, token)
}

func (c *segmentCollection) getSegment(end bool, token Token) *segment {
	var candWritable *segment
	var cand *segment
	col := c.beginning
	if end {
		col = c.end
	}
	for _, seg := range col {
		if seg.tokens.IsWithin(token) {
			if seg.writable {
				candWritable = seg
			}
			cand = seg
		}
	}

	// return a writable segment first (last)
	if candWritable != nil {
		return candWritable
	}
	return cand
}

func (c *segmentCollection) addSegment(newseg *segment) {
	// add at the end
	for _, seg := range c.end {
		if seg.positionEnd <= newseg.positionStart && newseg.tokens.IsOverlapsing(seg.tokens) {
			log.Debug("Added segment %s overlapse %s at end. Marking overlapsed as not writable!", newseg, seg)
			seg.writable = false

			seg.nextSegments = append(seg.nextSegments, newseg)
			newseg.prevSegments = append(newseg.prevSegments, seg)
		}
	}
	c.end = append(c.end, newseg)
	c.cleanup(true)

	// add at beggining
	for _, seg := range c.beginning {
		if newseg.positionEnd <= seg.positionStart && newseg.tokens.IsOverlapsing(seg.tokens) {
			log.Debug("Added segment %s overlapse %s at beginning.", newseg, seg)

			seg.prevSegments = append(seg.prevSegments, newseg)
			newseg.nextSegments = append(newseg.nextSegments, seg)
		}
	}
	c.beginning = append(c.beginning, newseg)
	c.cleanup(false)
}



//
// Removes all not writable segments that aren't covering any part of the range anymore 
//
func (c *segmentCollection) cleanup(end bool) {
	newSegs := make([]*segment, 0)

	var toClean []*segment
	if end {
		toClean = c.end
	} else {
		toClean = c.beginning
	}


	for o := 0; o < len(toClean); o++ {
		oSeg := toClean[o]

		// if the segment is not writable OR we are cleaning the beginning
		if !oSeg.writable || !end {
			covRange := oSeg.tokens
			useless := false

			// iterate on all segments, remove ranges covered from other segments from covRange
			for i := 0; i < len(toClean) && !useless; i++ {
				iSeg := toClean[i]

				// it's not the segment we are iterating on the outter loop + it's after or before (depending if we clean the end or beginning)
				if (i!=o) && ((end && iSeg.positionStart >= oSeg.positionEnd) || (!end && iSeg.positionEnd <= oSeg.positionStart)) {
					if iSeg.tokens.IsWithin(covRange.from) {
						covRange = TokenRange{iSeg.tokens.to, covRange.to}
					} 
					
					if iSeg.tokens.IsWithin(covRange.to) {
						covRange = TokenRange{covRange.from, iSeg.tokens.from}
					}
				}

				// as soon as the coverage range is <= 0, we know that this range is useless
				if covRange.Length() <= 0 {
					useless = true
					log.Debug("Segment %s now useless at end=%v. Removing it.", oSeg, end)
				}
			}

			// only add the segment to the new array if it isn't useless
			if !useless {
				newSegs = append(newSegs, oSeg)
			}


		} else if end {
			// if its a writable segment, add it
			newSegs = append(newSegs, oSeg)
		}
	}

	if end {
		c.end = newSegs
	} else {
		c.beginning = newSegs
	}
}


//
// Segment (file) in which entries are written sequentially
//
type segment struct {
	id           uint16
	tokens       TokenRange
	nextSegments []*segment
	prevSegments []*segment

	positionStart uint64 // absolute start position
	positionEnd   uint64 // absolute (exclusive) end of segment (ex: start=0, size=10, end=10)

	fd      *os.File
	lock	*sync.Mutex
	typedFd typedio.ReadWriter
	writable bool
}

func openSegment(path string) *segment {
	log.Info("Opening segment file %s", path)

	seg := &segment{
		nextSegments: make([]*segment, 0),
		prevSegments: make([]*segment, 0),
		writable: false,
		lock: new(sync.Mutex),
	}

	var err os.Error

	stat, err := os.Stat(path)
	if stat == nil || err != nil {
		log.Fatal("Couldn't stat segment file %s: %s", path, err)
	}

	var from, to Token
	_, err = fmt.Sscanf(stat.Name, "%016X_%04X_%04X.seg", &seg.positionStart, &from, &to)
	if err != nil {
		log.Fatal("Couldn't read segment file name %s: %s", path, err)
	}
	seg.tokens = TokenRange{from, to}

	seg.fd, err = os.Open(path)
	if err != nil {
		log.Fatal("Couldn't open segment %s: %s", path, err)
	}
	seg.typedFd = typedio.NewReadWriter(seg.fd)
	seg.positionEnd = seg.positionStart + uint64(stat.Size)

	return seg
}

func createSegment(dataDir string, token_from, token_to Token, position uint64) *segment {
	seg := &segment{
		nextSegments: make([]*segment, 0),
		prevSegments: make([]*segment, 0),
		tokens: TokenRange{token_from, token_to},
		positionStart: position,
		positionEnd: position,
		lock: new(sync.Mutex),
	}

	var err os.Error

	filePath := fmt.Sprintf("%s/%016X_%04X_%04X.seg", dataDir, position, token_from, token_to)
	seg.fd, err = os.Create(filePath)
	if err != nil {
		log.Fatal("Couldn't open segment %s: %s", filePath, err)
	}
	seg.typedFd = typedio.NewReadWriter(seg.fd)
	seg.writable = true

	return seg
}

func (s *segment) createEntry(token Token) *segmentEntry {
	entry := &segmentEntry{
		segment: s,
		token: token,
	}
	return entry
}

func (s *segment) replay(db *Db) (err os.Error) {
	log.Info("Replaying segment %s", s)

	entrych, errch := s.iter(0)

	end := false
	count := 0
	for !end {
		select {
		case entry, ok := <- entrych:
			if ok {
				count++
				err = entry.mutation.execute(db, true) // execute mutation (for replay)
				if err != nil {
					log.Error("Got an error replaying a mutation: %s", err)
					return
				}
			} else {
				end = true
			}

		case segerr, ok := <- errch:
			if ok {
				return segerr
			} else {
				end = true
			}
		}
	}

	log.Info("Segment %s replayed: %d mutations replayed", s, count)

	return
}

func (s *segment) iter(fromRelPosition uint64) (entrych chan *segmentEntry, errch chan os.Error) {
	entrych = make(chan *segmentEntry)
	errch = make(chan os.Error)

	go func() {
		curPos := fromRelPosition
		for {
			entry, err := s.read(curPos)

			if err != nil && err != os.EOF {
				errch <- err
				close(errch)
				return
			}

			if err == os.EOF {
				close(entrych)
				return
			}

			entrych <- entry
			curPos += uint64(entry.totalSize())
		}
		
	}()
	return entrych, errch
}

func (s *segment) read(relPosition uint64) (entry *segmentEntry, err os.Error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if relPosition >= (s.positionEnd - s.positionStart) {
		return nil, os.EOF
	}
	
	_, err = s.fd.Seek(int64(relPosition), 0)
	if err != nil {
		return nil, err
	}

	entry = &segmentEntry{
		segment: s,
	}
	err = entry.read(s.typedFd)
	if err != nil {
		return nil, err
	}

	if entry.position != (s.positionStart + relPosition) {
		return nil, os.NewError("Invalid segment entry")
	}

	return entry, err
}

func (s *segment) write(entry *segmentEntry) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.fd.Seek(int64(s.positionEnd-s.positionStart), 0)
	entry.position = s.positionEnd
	entry.write(s.typedFd)
	s.positionEnd += uint64(entry.totalSize())
}

func (s *segment) String() string {
	size := s.positionEnd - s.positionStart
	return fmt.Sprintf("Segment[from=%d, to=%d, writable=%v, pos_start=%d, pos_end=%d, size=%d]", s.tokens.from, s.tokens.to, s.writable, s.positionStart, s.positionEnd, size)
}

func (s *segment) toAbsolutePosition(relPosition uint32) uint64 {
	return s.positionStart + uint64(relPosition)
}


//
// Segment entry (a database mutation)
//
type segmentEntry struct {
	segment	 *segment

	position uint64
	token    Token

	mutsize  uint32
	mutation *mutation
}

func (e *segmentEntry) read(reader typedio.Reader) (err os.Error) {
	e.position, err = reader.ReadUint64() 		// position
	if err != nil {
		return err
	}

	tokenInt, err := reader.ReadUint16()		// token
	if err != nil {
		return err
	}
	e.token = Token(tokenInt)

	e.mutsize, err = reader.ReadUint32()	// size of mutation
	if err != nil {
		return err
	}

	e.mutation, err = mutationUnserialize(reader)	// mutation
	if err != nil {
		return err
	}
	e.mutation.segEntry = e
	e.mutation.seg = e.segment

	return
}

func (e *segmentEntry) write(writer typedio.Writer) (err os.Error) {
	err = writer.WriteUint64(e.position) 		// position
	if err != nil {
		return err
	}


	buf := buffer.New()
	err = e.mutation.serialize(buf)
	if err != nil {
		return err
	}

	e.mutsize = uint32(buf.Size)

	err = writer.WriteUint16(uint16(e.token)) 	// token
	if err != nil {
		return err
	}

	err = writer.WriteUint32(e.mutsize)       	// size of mutation
	if err != nil {
		return err
	}

	buf.Seek(0, 0)
	written, err := writer.Write(buf.Bytes()) 	// mutation
	if err != nil {
		return err
	}
	if written != int(e.mutsize) {
		return os.NewError(fmt.Sprintf("Couldn't write the whole mutation entry! Written %d of %d", written, e.mutsize))
	}


	return
}

func (e *segmentEntry) totalSize() uint32 {
	return 	8 + // pos
		2 + // token
		4 + // size
		e.mutsize // mut
}

func (e *segmentEntry) relativePosition() uint32 {
	return uint32(e.position - e.segment.positionStart)
}

func (e *segmentEntry) absolutePosition() uint64 {
	return e.position
}
