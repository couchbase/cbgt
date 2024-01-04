//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"os"

	"github.com/elazarl/go-bindata-assetfs"

	"github.com/gorilla/mux"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

// AssetFS returns the assetfs.AssetFS "filesystem" that holds static
// HTTP resources (css/html/js/images, etc) for the web UI.
//
// Users might introduce their own static HTTP resources and override
// resources from AssetFS() with their own resource lookup chaining.
func AssetFS() *assetfs.AssetFS {
	return assetFS()
}

// InitStaticRouter adds static HTTP resource routes to a router.
func InitStaticRouter(r *mux.Router, staticDir, staticETag string,
	pages []string, pagesHandler http.Handler) *mux.Router {
	return InitStaticRouterEx(r, staticDir, staticETag,
		pages, pagesHandler, nil)
}

// InitStaticRouterEx is like InitStaticRouter, but with optional
// manager parameter for more options.
func InitStaticRouterEx(r *mux.Router, staticDir, staticETag string,
	pages []string, pagesHandler http.Handler,
	mgr *cbgt.Manager) *mux.Router {
	prefix := ""
	if mgr != nil {
		prefix = mgr.GetOption("urlPrefix")
	}

	PIndexTypesInitRouter(r, "static.before", mgr)

	var s http.FileSystem
	if staticDir != "" {
		if _, err := os.Stat(staticDir); err == nil {
			log.Printf("http: serving assets from staticDir: %s", staticDir)
			s = http.Dir(staticDir)
		}
	}
	if s == nil {
		log.Printf("http: serving assets from embedded data")
		s = AssetFS()
	}

	staticRoutes := AssetNames()

	for _, route := range staticRoutes {
		// Add '/' prefix so the router doesn't flag paths that do
		// not begin with a slash.
		route = "/" + route
		r.Handle(prefix+route, http.StripPrefix(prefix+"/static/",
			ETagFileHandler{http.FileServer(s), staticETag}))
	}

	// Redirect any page the client asks for.
	for _, p := range pages {
		if pagesHandler != nil {
			r.Handle(p, pagesHandler)
		} else {
			r.Handle(p, RewriteURL("/", http.FileServer(s)))
		}
	}

	r.Handle(prefix+"/index.html",
		http.RedirectHandler(prefix+"/static/index.html", 302))
	r.Handle(prefix+"/",
		http.RedirectHandler(prefix+"/static/index.html", 302))

	PIndexTypesInitRouter(r, "static.after", mgr)

	return r
}

type ETagFileHandler struct {
	h    http.Handler
	etag string
}

func (mfh ETagFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if mfh.etag != "" {
		w.Header().Set("Etag", mfh.etag)
	}
	mfh.h.ServeHTTP(w, r)
}

// RewriteURL is a helper function that returns a URL path rewriter
// HandlerFunc, rewriting the URL path to a provided "to" string.
func RewriteURL(to string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = to
		h.ServeHTTP(w, r)
	})
}
