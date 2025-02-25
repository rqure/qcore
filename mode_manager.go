package main

import (
	"os"
	"strings"
)

type Mode string

const (
	ModeRead  Mode = "reader"
	ModeWrite Mode = "writer"
)

type ModeManager interface {
	HasModes(...Mode) bool
}

type modeManager struct {
	modes map[Mode]bool
}

func NewModeManager() ModeManager {
	me := &modeManager{
		modes: map[Mode]bool{},
	}

	me.loadModes()

	return me
}

func (me *modeManager) HasModes(modes ...Mode) bool {
	for _, mode := range modes {
		if !me.modes[mode] {
			return false
		}
	}

	return true
}

func (me *modeManager) loadModes() {
	modesEnv := os.Getenv("Q_MODES")
	strings.Split(modesEnv, ",")
	for _, mode := range strings.Split(modesEnv, ",") {
		mode = strings.ToLower(mode)
		me.modes[Mode(mode)] = true
	}
}
