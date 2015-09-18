package tsdmetrics

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/rcrowley/go-metrics"
)

// DuplicateMetric is the error returned by Registry.Register when a metric
// already exists.  If you mean to Register that metric you must first
// Unregister the existing metric.
type DuplicateStandardTaggedMetric struct {
	name string
	tags Tags
}

func (err DuplicateStandardTaggedMetric) Error() string {
	return fmt.Sprintf("duplicate metric: %s %s", err.name, err.tags.String())
}

// DuplicateMetric is the error returned by Registry.Register when a metric
// already exists.  If you mean to Register that metric you must first
// A Registry holds references to a set of metrics by name and can iterate
// over them, calling callback functions provided by the user.
//
// This is an interface so as to encourage other structs to implement
// the Registry API as appropriate.
type TaggedRegistry interface {

	// Call the given function for each registered metric.
	Each(func(string, StandardTaggedMetric))
	WrappedEach(func(string, StandardTaggedMetric) (string, StandardTaggedMetric), func(string, StandardTaggedMetric))

	// Get the metric by the given name or nil if none is registered.
	Get(string, Tags) interface{}

	// Gets an existing metric or registers the given one.
	// The interface can be the metric to register if not found in registry,
	// or a function returning the metric for lazy instantiation.
	GetOrRegister(string, Tags, interface{}) interface{}

	// Register the given metric under the given name.
	Register(string, Tags, interface{}) error

	// Run all registered healthchecks.
	RunHealthchecks()

	// Unregister the metric with the given name.
	Unregister(string, Tags)

	// Unregister all metrics.  (Mostly for testing.)
	UnregisterAll()
}

// The standard implementation of a Registry is a mutex-protected map
// of names to metrics.
type StandardTaggedRegistry struct {
	metrics map[string]map[TagsID]StandardTaggedMetric
	mutex   sync.Mutex
}

// Create a new registry.
func NewTaggedRegistry() TaggedRegistry {
	var r StandardTaggedRegistry
	r.metrics = make(map[string]map[TagsID]StandardTaggedMetric, 0)
	return &r
}

// Call the given function for each registered metric.
func (r *StandardTaggedRegistry) Each(f func(string, StandardTaggedMetric)) {
	for name, taggedMetrics := range r.registered() {
		for _, i := range taggedMetrics {
			f(name, i)
		}
	}
}

func (r *StandardTaggedRegistry) WrappedEach(wrapperFunc func(string, StandardTaggedMetric) (string, StandardTaggedMetric), f func(string, StandardTaggedMetric)) {
	for name, taggedMetrics := range r.registered() {
		for _, i := range taggedMetrics {
			f(wrapperFunc(name, i))
		}
	}
}

// Get the metric by the given name or nil if none is registered.
func (r *StandardTaggedRegistry) Get(name string, tags Tags) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if t, ok := r.metrics[name]; ok {
		if taggedMetric, ok := t[tags.TagsID()]; ok {
			return taggedMetric.Metric
		}
	}
	return nil
}

// Gets an existing metric or creates and registers a new one. Threadsafe
// alternative to calling Get and Register on failure.
// The interface can be the metric to register if not found in registry,
// or a function returning the metric for lazy instantiation.
func (r *StandardTaggedRegistry) GetOrRegister(name string, tags Tags, i interface{}) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if t, ok := r.metrics[name]; ok {
		if taggedMetric, ok := t[tags.TagsID()]; ok {
			return taggedMetric.Metric
		}
	}
	if v := reflect.ValueOf(i); v.Kind() == reflect.Func {
		i = v.Call(nil)[0].Interface()
	}
	r.register(name, tags, i)
	return i
}

// Register the given metric under the given name.  Returns a DuplicateMetric
// if a metric by the given name is already registered.
func (r *StandardTaggedRegistry) Register(name string, tags Tags, i interface{}) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.register(name, tags, i)
}

// Run all registered healthchecks.
func (r *StandardTaggedRegistry) RunHealthchecks() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, t := range r.metrics {
		for _, i := range t {
			if h, ok := i.Metric.(metrics.Healthcheck); ok {
				h.Check()
			}
		}
	}
}

// Unregister the metric with the given name.
func (r *StandardTaggedRegistry) Unregister(name string, tags Tags) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if t, ok := r.metrics[name]; ok {
		if _, ok := t[tags.TagsID()]; ok {
			delete(t, tags.TagsID())
		}

		if len(t) == 0 {
			delete(r.metrics, name)
		}
	}
}

// Unregister all metrics.  (Mostly for testing.)
func (r *StandardTaggedRegistry) UnregisterAll() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for name, _ := range r.metrics {
		delete(r.metrics, name)
	}
}

func (r *StandardTaggedRegistry) register(name string, tags Tags, i interface{}) error {
	if t, ok := r.metrics[name]; ok {
		if _, ok := t[tags.TagsID()]; ok {
			return DuplicateStandardTaggedMetric{name, tags}
		}
	}
	switch i.(type) {
	case metrics.Counter, metrics.Gauge, metrics.GaugeFloat64, metrics.Healthcheck, metrics.Histogram, metrics.Meter, metrics.Timer:
		if _, ok := r.metrics[name]; !ok {
			r.metrics[name] = make(map[TagsID]StandardTaggedMetric, 1)
		}
		taggedMetric := StandardTaggedMetric{Tags: tags, Metric: i}
		r.metrics[name][tags.TagsID()] = taggedMetric
	}
	return nil
}

func (r *StandardTaggedRegistry) registered() map[string]map[TagsID]StandardTaggedMetric {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	metrics := make(map[string]map[TagsID]StandardTaggedMetric, len(r.metrics))
	for name, i := range r.metrics {
		metrics[name] = i
	}
	return metrics
}

type PrefixedTaggedRegistry struct {
	underlying  TaggedRegistry
	prefix      string
	defaultTags Tags
}

func NewPrefixedTaggedRegistry(prefix string, tags Tags) TaggedRegistry {
	return &PrefixedTaggedRegistry{
		underlying:  NewTaggedRegistry(),
		prefix:      prefix,
		defaultTags: tags,
	}
}

// Call the given function for each registered metric.
func (r *PrefixedTaggedRegistry) Each(fn func(string, StandardTaggedMetric)) {
	wrapperFn := func(n string, m StandardTaggedMetric) (string, StandardTaggedMetric) {
		var realName string
		if r.prefix != "" {
			realName = fmt.Sprintf("%s.%s", r.prefix, n)
		} else {
			realName = n
		}

		newMetric := m.AddTags(r.defaultTags)

		return realName, newMetric
	}

	r.underlying.WrappedEach(wrapperFn, fn)
}

func (r *PrefixedTaggedRegistry) WrappedEach(wrapperFn func(string, StandardTaggedMetric) (string, StandardTaggedMetric), fn func(string, StandardTaggedMetric)) {
	r.underlying.WrappedEach(wrapperFn, fn)
}

// Get the metric by the given name or nil if none is registered.
func (r *PrefixedTaggedRegistry) Get(name string, tags Tags) interface{} {
	return r.underlying.Get(name, tags)
}

// Gets an existing metric or registers the given one.
// The interface can be the metric to register if not found in registry,
// or a function returning the metric for lazy instantiation.
func (r *PrefixedTaggedRegistry) GetOrRegister(name string, tags Tags, i interface{}) interface{} {
	return r.underlying.GetOrRegister(name, tags, i)
}

// Register the given metric under the given name. The name will be prefixed.
func (r *PrefixedTaggedRegistry) Register(name string, tags Tags, i interface{}) error {
	return r.underlying.Register(name, tags, i)
}

// Run all registered healthchecks.
func (r *PrefixedTaggedRegistry) RunHealthchecks() {
	r.underlying.RunHealthchecks()
}

// Unregister the metric with the given name. The name will be prefixed.
func (r *PrefixedTaggedRegistry) Unregister(name string, tags Tags) {
	r.underlying.Unregister(name, tags)
}

// Unregister all metrics.  (Mostly for testing.)
func (r *PrefixedTaggedRegistry) UnregisterAll() {
	r.underlying.UnregisterAll()
}

var DefaultTaggedRegistry TaggedRegistry = NewTaggedRegistry()

// Call the given function for each registered metric.
func TaggedEach(f func(string, StandardTaggedMetric)) {
	DefaultTaggedRegistry.Each(f)
}

// Get the metric by the given name or nil if none is registered.
func TaggedGet(name string, tags Tags) interface{} {
	return DefaultTaggedRegistry.Get(name, tags)
}

// Gets an existing metric or creates and registers a new one. Threadsafe
// alternative to calling Get and Register on failure.
func TaggedGetOrRegister(name string, tags Tags, i interface{}) interface{} {
	return DefaultTaggedRegistry.GetOrRegister(name, tags, i)
}

// Register the given metric under the given name.  Returns a DuplicateMetric
// if a metric by the given name is already registered.
func TaggedRegister(name string, tags Tags, i interface{}) error {
	return DefaultTaggedRegistry.Register(name, tags, i)
}

// Register the given metric under the given name.  Panics if a metric by the
// given name is already registered.
func TaggedMustRegister(name string, tags Tags, i interface{}) {
	if err := TaggedRegister(name, tags, i); err != nil {
		panic(err)
	}
}

// Run all registered healthchecks.
func TaggedRunHealthchecks() {
	DefaultTaggedRegistry.RunHealthchecks()
}

// Unregister the metric with the given name.
func TaggedUnregister(name string, tags Tags) {
	DefaultTaggedRegistry.Unregister(name, tags)
}
