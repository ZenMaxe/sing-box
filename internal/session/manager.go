package session

import (
	"sync"
	"time"
)

type Credential struct {
	MaxDevices int
	Revoked    bool
}

type Manager struct {
	lock           sync.Mutex
	activeSessions map[string][]Session
	credentialMap  map[string]Credential
}

type Session struct {
	ConnId     string
	Started    time.Time
	LastActive time.Time
}

func NewManager() *Manager {
	manager := &Manager{
		activeSessions: make(map[string][]Session),
		credentialMap:  make(map[string]Credential),
	}

	manager.StartCleaner(1*time.Minute, 5*time.Minute)

	return manager
}
func (m *Manager) SetCredentials(creds map[string]Credential) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.credentialMap = creds
}

func (m *Manager) OnConnect(uuid, connId string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	cred, ok := m.credentialMap[uuid]
	if !ok {
		sessions := m.activeSessions[uuid]
		session := Session{
			ConnId:  connId,
			Started: time.Now(),
		}
		sessions = append(sessions, session)
		m.activeSessions[uuid] = sessions
		return true
	}

	if cred.Revoked {
		return false
	}
	sessions := m.activeSessions[uuid]
	if len(sessions) >= cred.MaxDevices && cred.MaxDevices != 0 {
		return false
	}

	session := Session{
		ConnId:  connId,
		Started: time.Now(),
	}
	sessions = append(sessions, session)
	m.activeSessions[uuid] = sessions
	return true
}

func (m *Manager) OnDisconnect(uuid, connId string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	sessions, ok := m.activeSessions[uuid]
	if !ok {
		return
	}

	newSessions := make([]Session, 0, len(sessions))
	for _, s := range sessions {
		if s.ConnId != connId {
			newSessions = append(newSessions, s)
		}
	}

	if len(newSessions) == 0 {
		delete(m.activeSessions, uuid)
	} else {
		m.activeSessions[uuid] = newSessions
	}

}

func (m *Manager) Touch(uuid, connId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	sessions, ok := m.activeSessions[uuid]
	if !ok {
		return
	}
	for i := range sessions {
		if sessions[i].ConnId != connId {
			sessions[i].LastActive = time.Now()
		}
	}
}

func (m *Manager) StartCleaner(interval, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			m.cleanupExpired(ttl)
		}
	}()
}
func (m *Manager) cleanupExpired(ttl time.Duration) {
	m.lock.Lock()
	defer m.lock.Unlock()

	now := time.Now()
	for userId, sessList := range m.activeSessions {
		active := make([]Session, 0, len(sessList))

		for _, s := range sessList {
			if now.Sub(s.LastActive) < ttl {
				active = append(active, s)
			} else {
				// Optional: log or hook
				// log.Printf("Session expired: %s (%s)", userId, s.ConnId)
			}
		}

		if len(active) == 0 {
			delete(m.activeSessions, userId)
		} else {
			m.activeSessions[userId] = active
		}
	}
}
