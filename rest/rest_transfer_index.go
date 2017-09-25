//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package rest

import (
	"archive/tar"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// ---------------------------------------------------------------------

// PIndexContentHandler is a REST handler for retrieving the archived, compressed
// PIndex contents
type PIndexContentHandler struct {
	mgr *cbgt.Manager
}

// PIndexContentRequest represent the PIndex content request
type PIndexContentRequest struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// NewPIndexContentHandler returns a PIndexContentHandler
func NewPIndexContentHandler(mgr *cbgt.Manager) *PIndexContentHandler {
	return &PIndexContentHandler{mgr: mgr}
}

func (h *PIndexContentHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	piName := PIndexNameLookup(req)
	if piName == "" {
		ShowError(w, req, "rest_transfer_index: pindex name is required", http.StatusBadRequest)
		return
	}

	pi := h.mgr.GetPIndex(piName)
	if pi == nil {
		ShowError(w, req, "rest_transfer_index: no pindex found", http.StatusBadRequest)
		return
	}

	h.sendArchieve(h.mgr.PIndexPath(pi.Name), w, req)
}

func (h *PIndexContentHandler) sendArchieve(rootPath string,
	w http.ResponseWriter, req *http.Request) {
	_, err := ioutil.ReadDir(rootPath)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_transfer_index: pindex dir read failed, "+
			" err: %+v", err), http.StatusBadRequest)
		return
	}

	size, err := cbgt.GetDirectorySize(rootPath)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_transfer_index: pindex dir read failed, "+
			" err: %+v", err), http.StatusBadRequest)
		return
	}
	// Content-Length gets overridden by the "chunked" Transfer-Encoding.
	// Also avoiding Content-Length header since it was preventing  the
	// trailer headers.
	// By default the TE is "chunked"
	w.Header().Set("Content-Size", strconv.Itoa(int(size)))

	// fetch the encoding scheme requested by client
	// default is archived contents in tar form
	encodingScheme := req.Header.Get("Accept-Encoding")
	streamTarArchive(rootPath, encodingScheme, w, req)
}

func getWriter(scheme string, w http.ResponseWriter) io.WriteCloser {
	if strings.Contains(scheme, "gzip") {
		return gzip.NewWriter(w)
	} else if strings.Contains(scheme, "zlib") {
		return zlib.NewWriter(w)
	}
	return nil
}

func isCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// refactor this to be a generic exposed utility under misc?
func streamTarArchive(rootPath, encodingScheme string,
	w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Trailer", "Checksum")

	var tw *tar.Writer
	wc := getWriter(encodingScheme, w)
	if wc != nil {
		defer wc.Close()
		tw = tar.NewWriter(wc)
		w.Header().Set("Content-Encoding", encodingScheme)
	} else {
		tw = tar.NewWriter(w)
	}
	defer tw.Close()

	hash := md5.New()
	// hash and archive together
	mw := io.MultiWriter(tw, hash)

	ctx := req.Context()
	//buf := directio.AlignedBlock(directio.BlockSize * directio.BlockSize) // ~16 MB
	buf := make([]byte, 20*1024*1024)

	filepath.Walk(rootPath, func(file string, fi os.FileInfo, err error) error {
		if isCanceled(ctx) {
			log.Printf("rest_transfer_index: sendTarArchieve canceled, err: %+v", ctx.Err())
			ShowError(w, req, "rest_transfer_index: tar streaming cancelled", http.StatusBadRequest)
		}

		// return on any error
		if err != nil {
			return err
		}

		// return on directories since there will be no content to tar
		if fi.Mode().IsDir() {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		tarPath := file[len(rootPath):]
		tarPath = strings.TrimLeft(strings.Replace(tarPath, `\`, string(filepath.Separator), -1), `/`)
		header.Name = tarPath

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		//f, err := directio.OpenFile(file, os.O_RDONLY, 0666)
		f, err := os.OpenFile(file, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		// copy file data into tar writer
		if _, err := writeToStream(ctx, mw, f, buf); err != nil {
			log.Printf("rest_transfer_index: writeToStream file: %s, err: %+v", fi.Name(), err)
			return err
		}
		return nil
	})

	checkSum := hex.EncodeToString(hash.Sum(nil))
	w.Header().Set("Checksum", checkSum)
}

func writeToStream(ctx context.Context, dst io.Writer,
	src io.Reader, buf []byte) (written int64, err error) {
	for {
		if isCanceled(ctx) {
			return written, ctx.Err()
		}

		nr, er := io.ReadFull(src, buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF && er != io.ErrUnexpectedEOF {
				err = er
			}
			break
		}
	}

	return written, err
}
