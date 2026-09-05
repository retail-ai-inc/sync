package pipeline

// StoredLimits is where a batch's bounds come from when a deployment has set
// them, so an engine does not have to know about the control database.
//
// Nil, or a function returning zeroes, leaves the built-in defaults in place —
// which is what every engine did before, since none of them set Limits at all
// and MaxBytes therefore had no bound until it was given one.
var StoredLimits func() Limits

// limitsNow reports the bounds to apply, which is what was stored when
// anything was.
func limitsNow(configured Limits) Limits {
	if StoredLimits == nil {
		return configured
	}
	stored := StoredLimits()
	if configured.MaxEvents == 0 {
		configured.MaxEvents = stored.MaxEvents
	}
	if configured.MaxBytes == 0 {
		configured.MaxBytes = stored.MaxBytes
	}
	if configured.MaxTransactionEvents == 0 {
		configured.MaxTransactionEvents = stored.MaxTransactionEvents
	}
	return configured
}
