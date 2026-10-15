// Differential oracle: calls the pinned Go implementation, never a Go server.
package main

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/codec"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	psql "github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
	"os"
)

type item = [2]int64
type ancestor struct {
	BranchID               string
	BeginNodeID, EndNodeID int64
}
type reporter struct{}

func (reporter) Errorf(f string, a ...any) { panic(fmt.Sprintf(f, a...)) }
func (reporter) Fatalf(f string, a ...any) { panic(fmt.Sprintf(f, a...)) }
func branchResult(tree, branch string, ancestors []ancestor) any {
	if ancestors == nil {
		ancestors = []ancestor{}
	}
	values := []map[string]any{}
	for _, a := range ancestors {
		values = append(values, map[string]any{"branchID": a.BranchID, "beginNodeID": a.BeginNodeID, "endNodeID": a.EndNodeID})
	}
	return map[string]any{"treeID": tree, "branchID": branch, "ancestors": values}
}

type testCase struct {
	TreeID, BranchID, Token, NewBranchID string
	Ancestors                            []ancestor
	ForkNode                             int64
	Name, Op                             string
	Histories                            [][]item
	Current, Branch, Index               int
	Item                                 item
	Remote                               []item
	EventID                              int64
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
	case "branch_encode":
		branch := &workflow.HistoryBranch{TreeID: &c.TreeID, BranchID: &c.BranchID, Ancestors: []*workflow.HistoryBranchRange{}}
		for _, a := range c.Ancestors {
			a := a
			branch.Ancestors = append(branch.Ancestors, &workflow.HistoryBranchRange{BranchID: &a.BranchID, BeginNodeID: &a.BeginNodeID, EndNodeID: &a.EndNodeID})
		}
		token, e := codec.NewThriftRWEncoder().Encode(branch)
		if e != nil {
			return fail(e)
		}
		return hex.EncodeToString(token)
	case "branch_decode":
		token, e := hex.DecodeString(c.Token)
		if e != nil {
			return fail(e)
		}
		var branch workflow.HistoryBranch
		if e = codec.NewThriftRWEncoder().Decode(token, &branch); e != nil {
			return fail(e)
		}
		ancestors := []ancestor{}
		for _, a := range branch.Ancestors {
			ancestors = append(ancestors, ancestor{a.GetBranchID(), a.GetBeginNodeID(), a.GetEndNodeID()})
		}
		return branchResult(branch.GetTreeID(), branch.GetBranchID(), ancestors)
	case "branch_fork":
		ctrl := gomock.NewController(reporter{})
		defer ctrl.Finish()
		db := sqlplugin.NewMockDB(ctrl)
		parser := serialization.NewMockParser(ctrl)
		parser.EXPECT().HistoryTreeInfoToBlob(gomock.Any()).Return(p.DataBlob{}, nil)
		db.EXPECT().InsertIntoHistoryTree(gomock.Any(), gomock.Any()).Return(driver.RowsAffected(1), nil)
		store, e := psql.NewHistoryV2Persistence(db, nil, parser, nil)
		if e != nil {
			return fail(e)
		}
		source := types.HistoryBranch{TreeID: c.TreeID, BranchID: c.BranchID}
		for _, a := range c.Ancestors {
			source.Ancestors = append(source.Ancestors, &types.HistoryBranchRange{BranchID: a.BranchID, BeginNodeID: a.BeginNodeID, EndNodeID: a.EndNodeID})
		}
		response, e := store.ForkHistoryBranch(context.Background(), &p.InternalForkHistoryBranchRequest{ForkBranchInfo: source, ForkNodeID: c.ForkNode, NewBranchID: c.NewBranchID})
		if e != nil {
			return fail(e)
		}
		ancestors := []ancestor{}
		for _, a := range response.NewBranchInfo.Ancestors {
			ancestors = append(ancestors, ancestor{a.BranchID, a.BeginNodeID, a.EndNodeID})
		}
		return branchResult(response.NewBranchInfo.TreeID, response.NewBranchInfo.BranchID, ancestors)

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
