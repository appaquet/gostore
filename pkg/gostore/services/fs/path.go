package fs

import (
	"hash/adler32"
	"regexp"
	"strings"
)

var (
	pathRe = regexp.MustCompile("^(/([a-zA-Z0-9\\\\.]*))+(|/)$")
	Root   = NewPath("/")
)

type Path struct {
	path   string
	Static bool

	stdPath string
	Parts   []string
}

func NewPath(path string) *Path {
	inst := new(Path)

	path = strings.TrimSpace(path)

	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}

	inst.path = path
	inst.Static = true
	inst.stdPath = ""
	inst.Parts = make([]string, 0)

	inst.parse()

	return inst
}

func (p *Path) Hash() int { return int(adler32.Checksum([]byte(p.stdPath))) }

func (p *Path) String() string { return p.stdPath }

func (p *Path) Size() int16 { return int16(len(p.stdPath)) }

func (p *Path) Equals(path *Path) bool {
	if path.stdPath == p.stdPath {
		return true
	}

	return false
}

func (p *Path) Valid() bool {
	return pathRe.MatchString(p.path)
}

func (p *Path) parse() {
	if p.stdPath != p.path {
		parts := strings.Split(p.path, "/", -1)
		ptr := 0
		p.Parts = make([]string, len(parts))
		for i := 0; i < len(parts); i++ {
			part := strings.Trim(parts[i], " \n\t")
			switch part {
			case "..":
				ptr--
			case "", ".":
				// Do nothing
			default:
				p.Parts[ptr] = part
				ptr++
			}
		}

		if ptr <= 0 {
			p.Parts = []string{""}
		} else if ptr != len(parts) {
			p.Parts = p.Parts[0:ptr]
		}

		p.stdPath = "/" + strings.Join(p.Parts, "/")
	}
}

func (p *Path) IsRoot() bool {
	if len(p.Parts) == 0 || (len(p.Parts) == 1 && p.Parts[0] == "") {
		return true
	}

	return false
}

func (p *Path) ParentPath() *Path {
	if len(p.Parts) <= 1 {
		return Root
	}

	return NewPath(strings.Join(p.Parts[0:len(p.Parts)-1], "/"))
}

func (p *Path) BaseName() string {
	if len(p.Parts) <= 0 {
		return ""
	}

	return p.Parts[len(p.Parts)-1]
}

func (p *Path) ChildPath(child string) *Path {
	newparts := make([]string, len(p.Parts)+1)
	for i, part := range p.Parts {
		newparts[i] = part
	}
	newparts[len(newparts)-1] = child

	return NewPath(strings.Join(newparts, "/"))
}
