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
	failure, failed := from.detail.failures[tag]
	download, downloaded := from.detail.downloads[tag]
	var samples []Sample
	var links map[string]bool
	keep := from.detail.keeps[tag]
	if checks {
		samples = slices.Clone(from.detail.samples[tag])
		links = maps.Clone(from.detail.links[tag])
	}
	var loss *lossState
	if previous := from.detail.losses[tag]; losses && previous != nil {
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
			s.detail.failures[tag] = failure
		} else {
			delete(s.detail.failures, tag)
		}
		s.detail.samples[tag], s.detail.keeps[tag], s.detail.links[tag] = samples, keep, links
	}
	if downloads {
		if downloaded {
			s.detail.downloads[tag] = download
		} else {
			delete(s.detail.downloads, tag)
		}
	}
	if losses {
		if loss != nil {
			s.detail.losses[tag] = loss
		} else {
			delete(s.detail.losses, tag)
		}
	}
	s.notifyUpdated()
}
