package db



type ViewStateManager struct {
	vs	map[uint32]*ViewState
	pos	uint32
}

func newViewStateManager() *ViewStateManager {
	m := new(ViewStateManager)
	m.vs = make(map[uint32]*ViewState)
	return m
}

func (m *ViewStateManager) NewViewState() *ViewState {
	vs := new(ViewState)

	found := false
	for !found {
		if _, ok := m.vs[m.pos]; !ok {
			found = true
		} else {
			m.pos++
		}
	}

	m.vs[m.pos] = vs
	vs.id = m.pos
	return vs
}


type ViewState struct {
	id	uint32
	db	*Db
}



