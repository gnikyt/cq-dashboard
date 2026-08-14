package store

import "strings"

// Job names are supposed to be types ("import-rows"), with identity carried in
// attributes. Plenty of codebases put the identity in the name instead
// ("import-rows-8471"), which would turn a grouped view into thousands of
// groups of one. NormalizeName collapses the identifying parts so both
// conventions group usefully.

// NormalizeName reduces a job name to a pattern by replacing segments that
// look like identifiers with "*". Segments are split on "-", "_", ":", "/"
// and ".".
//
//	import-rows            -> import-rows
//	import-rows-8471       -> import-rows-*
//	sync:9f8a1c2d:shard-03 -> sync:*:shard-*
func NormalizeName(name string) string {
	if name == "" {
		return ""
	}

	var (
		out       strings.Builder
		segment   strings.Builder
		separator byte
		hasSep    bool
	)
	flush := func() {
		if hasSep {
			out.WriteByte(separator)
		}
		out.WriteString(maskSegment(segment.String()))
		segment.Reset()
	}

	for i := 0; i < len(name); i++ {
		switch c := name[i]; c {
		case '-', '_', ':', '/', '.':
			flush()
			separator, hasSep = c, true
		default:
			segment.WriteByte(c)
		}
	}
	flush()
	return out.String()
}

// maskSegment returns "*" when a segment reads as an identifier rather than a
// word: anything containing a digit, or a long hex run such as a UUID chunk.
func maskSegment(segment string) string {
	if segment == "" {
		return segment
	}

	digits, hex := 0, 0
	for i := 0; i < len(segment); i++ {
		c := segment[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
			hex++
		case (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			hex++
		}
	}

	// Any digit makes it an identifier... "v2" is the rare false positive and
	// grouping "api-v2-*" is still the useful answer.
	if digits > 0 {
		return "*"
	}
	// A long all-hex run is a UUID or checksum fragment.
	if hex == len(segment) && len(segment) >= 8 {
		return "*"
	}
	return segment
}
