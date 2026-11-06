package urltest

import (
	"maps"
	"slices"
)

// CopyMemberFrom copies the requested evidence for one unchanged member into a
// successor's store. Callers must match the member object and the URLs/schedule
// before copying checks, the download URL before downloads, and the loss URL
// before losses. Mutable samples and maps are copied, never shared with a round
// that may still be finishing in the retiring group.
func (s *HistoryStorage) CopyMemberFrom(from *HistoryStorage, tag string, checks, downloads, losses bool) {
	if from == nil || from == s {
		return
	}
	from.access.RLock()
	history := from.delayHistory[tag]
	if checks && history != nil {
		copy := *history
		history = &copy
	}
	failure, failed := from.failures[tag]
	download, downloaded := from.downloads[tag]
	var samples []Sample
	var links map[string]bool
	keep := from.keeps[tag]
	if checks {
		samples = slices.Clone(from.samples[tag])
		links = maps.Clone(from.links[tag])
	}
	var loss *lossState
	if previous := from.losses[tag]; losses && previous != nil {
		copy := *previous
		copy.samples = slices.Clone(previous.samples)
		loss = &copy
	}
	from.access.RUnlock()

	s.access.Lock()
	defer s.access.Unlock()
	if checks {
		s.delayHistory[tag] = history
		if failed {
			s.failures[tag] = failure
		} else {
			delete(s.failures, tag)
		}
		s.samples[tag], s.keeps[tag], s.links[tag] = samples, keep, links
	}
	if downloads {
		if downloaded {
			s.downloads[tag] = download
		} else {
			delete(s.downloads, tag)
		}
	}
	if losses {
		if loss != nil {
			s.losses[tag] = loss
		} else {
			delete(s.losses, tag)
		}
	}
	s.notifyUpdated()
}
