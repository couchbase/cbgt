package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type KvEntry struct {
	Path  string
	Value []byte
	Rev   []byte
}

type dataEntry struct {
	K KvEntry
	w *http.ResponseWriter
}

type listEntry struct {
	path string
	b    *bufio.ReadWriter
}

var store map[string]*dataEntry
var list []*listEntry
var rpcBody string = "{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"AuthCacheSvc.UpdateDB\",\"params\":[{\"specialUser\":\"@fts-cbauth\",\"nodes\":[{\"host\":\"127.0.0.1\",\"user\":\"_admin\",\"password\":\"c55756e3cf12269e4b49eb8cba10389d\",\"ports\":[9000,9200,9100,9101,9102,9103,9104,9105,9500,10000,12000,12001,9499],\"local\":true}],\"buckets\":[{\"name\":\"gamesim-sample\",\"password\":\"\"}],\"tokenCheckURL\":\"http://127.0.0.1:9000/_cbauth\",\"admin\":{\"user\":\"Administrator\",\"salt\":\"h+bTDx1y7VfNymzHos93kQ==\",\"mac\":\"XFaBj6SzQyzkNYMnRzt117OAhB0=\"}}]}"

func sendData(w http.ResponseWriter, p string) {
	data := []byte{}
	for k, v := range store {
		if strings.HasPrefix(k, p) && v.K.Rev != nil {
			fmt.Printf("sending data from all data %v", v.K)
			val, _ := json.Marshal(v.K)
			data = append(data, val...)
		}
	}
	w.Write(data)
}

func sendAllData(w http.ResponseWriter, p string) {
	var header string = "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Server: Couchbase Server\r\n" +
		"Pragma: no-cache\r\n" +
		"Content-Type: application/json; charset=utf-8\r\n" +
		"Cache-Control: no-cache\r\n\r\n"
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}
	_, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bufrw.WriteString(header)
	bufrw.Flush()
	list = append(list, &listEntry{p, bufrw})
	for k, v := range store {
		if strings.HasPrefix(k, p) && v.K.Rev != nil {
			fmt.Printf("sending data from all data %v", v.K)
			val, _ := json.Marshal(v.K)
			bufrw.WriteString(fmt.Sprintf("%x", len(val)) + "\r\n")
			bufrw.Write(val)
			bufrw.WriteString("\r\n")
		}
	}
}

func checkList(path string, body []byte) {
	for _, val := range list {
		if strings.HasPrefix(val.path, path) {
			fmt.Printf("sending body %v", body)
			bufrw := val.b
			bufrw.WriteString(fmt.Sprintf("%x", len(body)) + "\r\n")
			bufrw.Write(body)
			bufrw.WriteString("\r\n")
		}
	}
}

func HandleRPC(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", 
            http.StatusInternalServerError)
		return
	}
	_, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
    // send a dummy response for rpc connect. We should not close the 
    // connection else EOF error is thrown by cbauth client.
    data := rpcBody
	var header string = "HTTP/1.1 200 OK\r\n" +
		"Server: Couchbase Server\r\n" +
		"Content-Length: %d\r\n" +
		"Content-Type: application/json; charset=utf-8\r\n\r\n"
	bufrw.WriteString(fmt.Sprintf(header, len(data)))
	bufrw.Write([]byte(data))
	bufrw.WriteString("\r\n")
	bufrw.Flush()
	val := make([]byte, 1000)
	go func() {
		for {
			bufrw.Read(val)
		}
	}()
}

func parseBody(body string) map[string]string {
	m := make(map[string]string)
	for _, v := range strings.Split(body, "&") {
		k := strings.Split(v, "=")
		m[k[0]] = k[1]
	}
	return m
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("got request ", r.URL.Path)
	if r.Method == "RPCCONNECT" {
		HandleRPC(w)
		return
	}
	if r.Method == "PUT" {
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Printf("got body %s", body)
		b := parseBody(string(body))
		x, _ := url.Parse(b["value"])
		v := x.Path
		en := KvEntry{
			Path:  r.URL.Path,
			Value: []byte(v),
			Rev:   []byte("1"),
		}
		fmt.Printf("storing entry is %v", en)
		store[r.URL.Path] = &dataEntry{K: en}
		w.WriteHeader(http.StatusOK)
		val, _ := json.Marshal(en)
		checkList(r.URL.Path, val)
		return
	}
	query := r.URL.Query()
	fmt.Println("query params are ", query, "path ", r.URL.Path)
	for _, v := range query["feed"] {
		if v == "continuous" {
			fmt.Println("inside continuous")
			sendAllData(w, r.URL.Path)
		}
		return
	}
	if r.URL.Path[len(r.URL.Path)-1] == '/' {
		sendData(w, r.URL.Path)
	} else {
		v := store[r.URL.Path]
		if v == nil {
			val, _ := json.Marshal(dataEntry{})
			w.Write(val)
			return
		}
		val, _ := json.Marshal(v.K)
		w.Write(val)
	}
}

func main() {
	store = make(map[string]*dataEntry)
	http.HandleFunc("/", viewHandler)
	http.ListenAndServe(":9000", nil)
}
