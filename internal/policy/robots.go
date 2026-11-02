package policy

import (
	"bufio"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

const maxRobotsBytes = 512 * 1024

// ParseRobots selects the most specific matching user-agent groups and merges
// equally specific groups. A malformed or oversized response is unreadable.
func ParseRobots(input io.Reader, agent string, expires time.Time) (Snapshot, error) {
	if strings.TrimSpace(agent) == "" || expires.IsZero() {
		return Snapshot{}, errors.New("invalid robots metadata")
	}
	raw, err := io.ReadAll(io.LimitReader(input, maxRobotsBytes+1))
	if err != nil || len(raw) > maxRobotsBytes {
		return Snapshot{}, errors.New("unreadable robots")
	}
	type group struct {
		agents []string
		rules  []Rule
	}
	groups := []group{}
	current := group{}
	directives := false
	flush := func() {
		if len(current.agents) > 0 {
			groups = append(groups, current)
		}
		current = group{}
		directives = false
	}
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 4096), maxRobotsBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if line == "" {
			continue
		}
		pair := strings.SplitN(line, ":", 2)
		if len(pair) != 2 {
			return Snapshot{}, errors.New("malformed robots directive")
		}
		key, value := strings.ToLower(strings.TrimSpace(pair[0])), strings.TrimSpace(pair[1])
		if key == "user-agent" {
			if directives {
				flush()
			}
			if value == "" {
				return Snapshot{}, errors.New("empty robots user agent")
			}
			current.agents = append(current.agents, strings.ToLower(value))
			continue
		}
		if len(current.agents) == 0 {
			continue
		}
		directives = true
		if key == "allow" || key == "disallow" {
			if value == "" {
				continue
			}
			if !strings.HasPrefix(value, "/") {
				return Snapshot{}, errors.New("invalid robots path")
			}
			current.rules = append(current.rules, Rule{Path: value, Allow: key == "allow"})
		}
	}
	if scanner.Err() != nil {
		return Snapshot{}, errors.New("unreadable robots")
	}
	flush()
	best := -1
	rules := []Rule{}
	agent = strings.ToLower(agent)
	for _, g := range groups {
		score := -1
		for _, a := range g.agents {
			if a == "*" {
				if score < 0 {
					score = 0
				}
			} else if strings.Contains(agent, a) && len(a) > score {
				score = len(a)
			}
		}
		if score < 0 {
			continue
		}
		if score > best {
			best = score
			rules = nil
		}
		if score == best {
			rules = append(rules, g.rules...)
		}
	}
	return Snapshot{Readable: true, ExpiresAt: expires, Rules: rules}, nil
}

// robotsMatch returns specificity in literal bytes, or -1 for no match.
func robotsMatch(pattern, path string) int {
	end := strings.HasSuffix(pattern, "$")
	if end {
		pattern = strings.TrimSuffix(pattern, "$")
	}
	parts := strings.Split(pattern, "*")
	for i := range parts {
		parts[i] = regexp.QuoteMeta(parts[i])
	}
	expression := "^" + strings.Join(parts, ".*")
	if end {
		expression += "$"
	}
	matched, err := regexp.MatchString(expression, path)
	if err != nil || !matched {
		return -1
	}
	return len(strings.ReplaceAll(pattern, "*", ""))
}
