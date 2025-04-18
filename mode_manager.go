package main

import (
	"os"
	"strings"
)

type Mode string

const (
	// Reader mode is the scalable mode. In this mode, the application will respond to read requests.
	// Multiple readers can be added to the cluster to scale the read capacity.
	ModeRead Mode = "reader"

	// Writer mode is the consistent mode. In this mode, the application will respond to write requests.
	// Only one writer can be added to the cluster to ensure consistency.
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

	if modesEnv == "" {
		modesEnv = string(ModeRead) + "," + string(ModeWrite)
	}

	strings.Split(modesEnv, ",")
	for _, mode := range strings.Split(modesEnv, ",") {
		mode = strings.ToLower(mode)
		me.modes[Mode(mode)] = true
	}
}
