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
	
	for _, ok := m.vs[pos]; !ok {
		pos++
	}

	m.vs[pos] = vs
	vs.id = pos
	return vs
}


type ViewState struct {
	id	uint32
}

