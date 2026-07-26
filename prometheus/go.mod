module github.com/townbell/bus/prometheus

go 1.21

require (
	github.com/prometheus/client_golang v1.17.0
	// Must be v0.5.0 or newer. Up to and including v0.4.0 the root module still
	// bundled this package, so an older requirement makes the import path
	// ambiguous: it would be provided by two modules at once.
	github.com/townbell/bus v0.5.0
)

// v0.5.0 required github.com/townbell/bus v0.4.0, which still contains this
// package, so importing the adapter failed with "ambiguous import".
retract v0.5.0

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	golang.org/x/sys v0.11.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

// Keep the adapter building against the parent module in this repository.
// Replace directives are ignored by consumers, which resolve the require above.
replace github.com/townbell/bus => ../
