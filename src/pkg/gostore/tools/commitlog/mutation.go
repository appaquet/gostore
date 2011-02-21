package commitlog

import (
	"gostore/tools/typedio"
)

type Mutation interface {
	Unserialize(reader typedio.Reader)
	Serialize(writer typedio.Writer)
	Commit()
	Execute()
}
