// Differential oracle: calls the pinned Go implementation, never a Go server.
package main

import (
	"encoding/json"
	"fmt"
	p "github.com/uber/cadence/common/persistence"
	"os"
)

type item = [2]int64
type testCase struct {
	Name, Op               string
	Histories              [][]item
	Current, Branch, Index int
	Item                   item
	Remote                 []item
	EventID                int64
}

func history(items []item) *p.VersionHistory {
	values := make([]*p.VersionHistoryItem, 0, len(items))
	for _, i := range items {
		values = append(values, p.NewVersionHistoryItem(i[0], i[1]))
	}
	return p.NewVersionHistory([]byte("branch"), values)
}
func pair(i *p.VersionHistoryItem) any {
	if i == nil {
		return nil
	}
	return item{i.EventID, i.Version}
}
func pairs(h *p.VersionHistory) []item {
	out := []item{}
	for _, i := range h.Items {
		out = append(out, item{i.EventID, i.Version})
	}
	return out
}
func evaluate(c testCase) (result any) {
	defer func() {
		if value := recover(); value != nil {
			result = map[string]any{"panic": fmt.Sprint(value)}
		}
	}()
	h := &p.VersionHistories{CurrentVersionHistoryIndex: c.Current}
	for _, values := range c.Histories {
		h.Histories = append(h.Histories, history(values))
	}
	var err error
	fail := func(e error) any { return map[string]any{"error": e.Error()} }
	switch c.Op {
	case "new_item":
		return pair(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
	case "add":
		err = h.Histories[c.Branch].AddOrUpdateItem(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
		if err != nil {
			return fail(err)
		}
		return pairs(h.Histories[c.Branch])
	case "contains":
		return h.Histories[c.Branch].ContainsItem(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
	case "lca":
		i, e := h.Histories[c.Branch].FindLCAItem(history(c.Remote))
		if e != nil {
			return fail(e)
		}
		return pair(i)
	case "duplicate":
		branch, e := h.Histories[c.Branch].DuplicateUntilLCAItem(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
		if e != nil {
			return fail(e)
		}
		return map[string]any{"items": pairs(branch), "tokenLength": len(branch.BranchToken)}
	case "appendable":
		return h.Histories[c.Branch].IsLCAAppendable(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
	case "first":
		i, e := h.Histories[c.Branch].GetFirstItem()
		if e != nil {
			return fail(e)
		}
		return pair(i)
	case "last":
		i, e := h.Histories[c.Branch].GetLastItem()
		if e != nil {
			return fail(e)
		}
		return pair(i)
	case "event_version":
		v, e := h.Histories[c.Branch].GetEventVersion(c.EventID)
		if e != nil {
			return fail(e)
		}
		return v
	case "get":
		_, e := h.GetVersionHistory(c.Index)
		if e != nil {
			return fail(e)
		}
		return c.Index
	case "add_history", "add_null":
		var incoming *p.VersionHistory
		if c.Op != "add_null" {
			incoming = history(c.Remote)
		}
		changed, index, e := h.AddVersionHistory(incoming)
		if e != nil {
			return fail(e)
		}
		all := [][]item{}
		for _, branch := range h.Histories {
			all = append(all, pairs(branch))
		}
		return map[string]any{"changed": changed, "index": index, "current": h.CurrentVersionHistoryIndex, "histories": all}
	case "lca_history":
		index, i, e := h.FindLCAVersionHistoryIndexAndItem(history(c.Remote))
		if e != nil {
			return fail(e)
		}
		return map[string]any{"index": index, "item": pair(i)}
	case "find_first":
		index, _, e := h.FindFirstVersionHistoryByItem(p.NewVersionHistoryItem(c.Item[0], c.Item[1]))
		if e != nil {
			return fail(e)
		}
		return index
	case "rebuilt":
		v, e := h.IsRebuilt()
		if e != nil {
			return fail(e)
		}
		return v
	case "set_current":
		err = h.SetCurrentVersionHistoryIndex(c.Index)
		if err != nil {
			return fail(err)
		}
		return h.GetCurrentVersionHistoryIndex()
	default:
		panic("unknown oracle operation " + c.Op)
	}
}
func main() {
	var cases []testCase
	if err := json.NewDecoder(os.Stdin).Decode(&cases); err != nil {
		panic(err)
	}
	out := make([]any, 0, len(cases))
	for _, c := range cases {
		out = append(out, evaluate(c))
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		panic(err)
	}
}
