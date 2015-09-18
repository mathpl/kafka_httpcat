package tsdmetrics

import (
	"bytes"
	"fmt"
	"sort"
)

type StandardTaggedMetric struct {
	Tags   Tags
	Metric interface{}
}

func (stm *StandardTaggedMetric) GetTagsID() TagsID {
	return stm.GetTagsID()
}

func (stm *StandardTaggedMetric) AddTags(tags Tags) StandardTaggedMetric {
	var newStm StandardTaggedMetric

	newStm.Metric = stm.Metric
	newStm.Tags = stm.Tags.AddTags(tags)

	return newStm
}

func (stm *StandardTaggedMetric) TagString() string {
	return stm.Tags.String()
}

type TagsID string
type Tags map[string]string

func (tm Tags) TagsID() TagsID {
	keys := make([]string, len(tm))
	i := 0
	for k, _ := range tm {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	var tagid bytes.Buffer
	for _, k := range keys {
		tagid.Write([]byte(fmt.Sprintf("%s=%s;", k, tm[k])))
	}

	return TagsID(tagid.String())
}

func (tm Tags) String() string {
	var tagid bytes.Buffer
	for k, v := range tm {
		tagid.Write([]byte(fmt.Sprintf("%s=%s ", k, v)))
	}

	return tagid.String()
}

func (tm Tags) AddTags(tags Tags) Tags {
	newTags := make(map[string]string, len(tm)+len(tags))
	for t, v := range tm {
		newTags[t] = v
	}

	for t, v := range tags {
		if _, ok := newTags[t]; !ok {
			newTags[t] = v
		}
	}

	return newTags
}
