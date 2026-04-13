package main

import (
	"archive/zip"
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port            string
	OSSEndpoint     string
	OSSBucket       string
	OSSAccessKeyID  string
	OSSAccessSecret string
	OSSPrefix       string
	MaxUploadMB     int64
}

type Server struct {
	cfg    Config
	client *OSSClient
}

type BackupItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	Size      int64  `json:"size"`
	FileName  string `json:"file_name"`
}

type OSSClient struct {
	endpoint  string
	bucket    string
	accessKey string
	secretKey string
	http      *http.Client
}

type listBucketResult struct {
	XMLName     xml.Name  `xml:"ListBucketResult"`
	Contents    []content `xml:"Contents"`
	IsTruncated bool      `xml:"IsTruncated"`
	NextMarker  string    `xml:"NextMarker"`
}

type content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	client := &OSSClient{
		endpoint:  strings.TrimPrefix(strings.TrimPrefix(cfg.OSSEndpoint, "https://"), "http://"),
		bucket:    cfg.OSSBucket,
		accessKey: cfg.OSSAccessKeyID,
		secretKey: cfg.OSSAccessSecret,
		http:      &http.Client{Timeout: 60 * time.Second},
	}

	s := &Server{cfg: cfg, client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/api/backups", s.backupsEntry)
	mux.HandleFunc("/api/backups/", s.downloadBackup)

	addr := ":" + cfg.Port
	log.Printf("server started at %s", addr)
	if err := http.ListenAndServe(addr, loggingMiddleware(mux)); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

func loadConfig() (Config, error) {
	maxMB := int64(100)
	if v := strings.TrimSpace(os.Getenv("MAX_UPLOAD_MB")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("invalid MAX_UPLOAD_MB")
		}
		maxMB = n
	}

	cfg := Config{
		Port:            envOrDefault("PORT", "8080"),
		OSSEndpoint:     strings.TrimSpace(os.Getenv("OSS_ENDPOINT")),
		OSSBucket:       strings.TrimSpace(os.Getenv("OSS_BUCKET")),
		OSSAccessKeyID:  strings.TrimSpace(os.Getenv("OSS_ACCESS_KEY_ID")),
		OSSAccessSecret: strings.TrimSpace(os.Getenv("OSS_ACCESS_KEY_SECRET")),
		OSSPrefix:       envOrDefault("OSS_PREFIX", "backups/"),
		MaxUploadMB:     maxMB,
	}

	if cfg.OSSEndpoint == "" || cfg.OSSBucket == "" || cfg.OSSAccessKeyID == "" || cfg.OSSAccessSecret == "" {
		return Config{}, errors.New("OSS_ENDPOINT, OSS_BUCKET, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET are required")
	}
	if !strings.HasSuffix(cfg.OSSPrefix, "/") {
		cfg.OSSPrefix += "/"
	}

	return cfg, nil
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) backupsEntry(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.uploadBackup(w, r)
	case http.MethodGet:
		s.listBackups(w)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) uploadBackup(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadMB*1024*1024)
	if err := r.ParseMultipartForm(s.cfg.MaxUploadMB * 1024 * 1024); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form or payload too large")
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if !regexp.MustCompile(`^[\w\-\u4e00-\u9fa5 ]{1,64}$`).MatchString(name) {
		writeError(w, http.StatusBadRequest, "name must be 1-64 chars and contains letters, numbers, spaces, -, _")
		return
	}

	sqliteFile, sqliteHeader, err := r.FormFile("sqlite")
	if err != nil {
		writeError(w, http.StatusBadRequest, "sqlite file is required (field: sqlite)")
		return
	}
	defer sqliteFile.Close()
	jsonFile, jsonHeader, err := r.FormFile("json")
	if err != nil {
		writeError(w, http.StatusBadRequest, "json file is required (field: json)")
		return
	}
	defer jsonFile.Close()

	archiveData, archiveName, err := buildArchive(name, sqliteHeader, sqliteFile, jsonHeader, jsonFile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	now := time.Now().UTC()
	id := now.Format("20060102T150405Z")
	key := s.cfg.OSSPrefix + id + "-" + sanitizeName(name) + ".zip"

	meta := map[string]string{
		"x-oss-meta-backup-name":       name,
		"x-oss-meta-backup-created-at": now.Format(time.RFC3339),
		"x-oss-meta-backup-id":         id,
	}
	if err := s.client.PutObject(key, archiveData, "application/zip", meta); err != nil {
		writeError(w, http.StatusInternalServerError, "upload to OSS failed")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         id,
		"name":       name,
		"created_at": now.Format(time.RFC3339),
		"object_key": key,
		"file_name":  archiveName,
	})
}

func buildArchive(name string, sqliteHeader *multipart.FileHeader, sqliteFile multipart.File, jsonHeader *multipart.FileHeader, jsonFile multipart.File) ([]byte, string, error) {
	sqliteData, err := io.ReadAll(sqliteFile)
	if err != nil {
		return nil, "", errors.New("read sqlite failed")
	}
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, "", errors.New("read json failed")
	}
	if !json.Valid(jsonData) {
		return nil, "", errors.New("json file is not valid JSON")
	}
	buf := bytes.NewBuffer(nil)
	zw := zip.NewWriter(buf)

	sqliteName := safeFileName(sqliteHeader.Filename)
	if sqliteName == "" {
		sqliteName = "data.sqlite"
	}
	jsonName := safeFileName(jsonHeader.Filename)
	if jsonName == "" {
		jsonName = "data.json"
	}
	if err := addZipFile(zw, sqliteName, sqliteData); err != nil {
		return nil, "", err
	}
	if err := addZipFile(zw, jsonName, jsonData); err != nil {
		return nil, "", err
	}
	manifest := map[string]string{"name": name, "created_at": time.Now().UTC().Format(time.RFC3339), "sqlite": sqliteName, "json": jsonName}
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := addZipFile(zw, "manifest.json", manifestBytes); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", errors.New("build zip failed")
	}
	return buf.Bytes(), fmt.Sprintf("%s-%s.zip", time.Now().UTC().Format("20060102T150405Z"), sanitizeName(name)), nil
}

func addZipFile(zw *zip.Writer, name string, content []byte) error {
	w, err := zw.Create(name)
	if err != nil {
		return errors.New("create zip entry failed")
	}
	if _, err := w.Write(content); err != nil {
		return errors.New("write zip entry failed")
	}
	return nil
}

func (s *Server) listBackups(w http.ResponseWriter) {
	marker := ""
	items := make([]BackupItem, 0)
	for {
		res, err := s.client.ListObjects(s.cfg.OSSPrefix, marker, 1000)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list objects failed")
			return
		}
		for _, obj := range res.Contents {
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}
			headers, err := s.client.HeadObject(obj.Key)
			if err != nil {
				continue
			}
			name := headers.Get("X-Oss-Meta-Backup-Name")
			if name == "" {
				name = extractNameFromKey(path.Base(obj.Key))
			}
			id := headers.Get("X-Oss-Meta-Backup-Id")
			if id == "" {
				id = strings.TrimSuffix(path.Base(obj.Key), path.Ext(obj.Key))
			}
			createdAt := headers.Get("X-Oss-Meta-Backup-Created-At")
			if createdAt == "" {
				createdAt = obj.LastModified
			}
			items = append(items, BackupItem{ID: id, Name: name, CreatedAt: createdAt, Size: obj.Size, FileName: path.Base(obj.Key)})
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}

	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt > items[j].CreatedAt })
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) downloadBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/backups/"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "backup id is required")
		return
	}
	key, fileName, err := s.findObjectByID(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	body, err := s.client.GetObject(key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fetch backup failed")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, fileName))
	if _, err := io.Copy(w, body); err != nil {
		log.Printf("stream backup failed: %v", err)
	}
}

func (s *Server) findObjectByID(id string) (string, string, error) {
	marker := ""
	for {
		res, err := s.client.ListObjects(s.cfg.OSSPrefix, marker, 1000)
		if err != nil {
			return "", "", err
		}
		for _, obj := range res.Contents {
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}
			base := path.Base(obj.Key)
			if base == id || strings.TrimSuffix(base, path.Ext(base)) == id {
				return obj.Key, base, nil
			}
			headers, err := s.client.HeadObject(obj.Key)
			if err == nil && headers.Get("X-Oss-Meta-Backup-Id") == id {
				return obj.Key, base, nil
			}
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	return "", "", errors.New("not found")
}

func (c *OSSClient) ListObjects(prefix, marker string, maxKeys int) (*listBucketResult, error) {
	q := url.Values{}
	q.Set("prefix", prefix)
	q.Set("max-keys", strconv.Itoa(maxKeys))
	if marker != "" {
		q.Set("marker", marker)
	}
	resp, err := c.doRequest(http.MethodGet, "", q, nil, "", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("oss list failed: %d %s", resp.StatusCode, string(b))
	}
	var result listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *OSSClient) PutObject(key string, body []byte, contentType string, headers map[string]string) error {
	resp, err := c.doRequest(http.MethodPut, key, nil, bytes.NewReader(body), contentType, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("oss put failed: %d %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *OSSClient) HeadObject(key string) (http.Header, error) {
	resp, err := c.doRequest(http.MethodHead, key, nil, nil, "", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("head object failed: %d", resp.StatusCode)
	}
	return resp.Header, nil
}

func (c *OSSClient) GetObject(key string) (io.ReadCloser, error) {
	resp, err := c.doRequest(http.MethodGet, key, nil, nil, "", nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, fmt.Errorf("get object failed: %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (c *OSSClient) doRequest(method, objectKey string, query url.Values, body io.Reader, contentType string, extraHeaders map[string]string) (*http.Response, error) {
	date := time.Now().UTC().Format(http.TimeFormat)
	canonicalizedOSSHeaders := canonicalOSSHeaders(extraHeaders)
	canonicalizedResource := "/" + c.bucket + "/" + objectKey
	if query != nil && len(query) > 0 {
		canonicalizedResource += "?" + query.Encode()
	}

	stringToSign := method + "\n\n" + contentType + "\n" + date + "\n" + canonicalizedOSSHeaders + canonicalizedResource
	h := hmac.New(sha1.New, []byte(c.secretKey))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	u := "https://" + c.bucket + "." + c.endpoint + "/" + objectKey
	if query != nil && len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Date", date)
	req.Header.Set("Authorization", "OSS "+c.accessKey+":"+signature)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return c.http.Do(req)
}

func canonicalOSSHeaders(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	keys := make([]string, 0, len(headers))
	for k := range headers {
		lk := strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(lk, "x-oss-") {
			keys = append(keys, lk)
		}
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, k := range keys {
		lines = append(lines, k+":"+strings.TrimSpace(headers[k]))
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start))
	})
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
func sanitizeName(v string) string {
	v = strings.TrimSpace(strings.ReplaceAll(v, " ", "-"))
	v = regexp.MustCompile(`[^a-zA-Z0-9_\-\u4e00-\u9fa5]`).ReplaceAllString(v, "")
	if v == "" {
		return "backup"
	}
	return v
}
func safeFileName(v string) string {
	v = path.Base(strings.TrimSpace(v))
	if v == "." || v == "/" {
		return ""
	}
	return strings.ReplaceAll(v, "..", "")
}
func extractNameFromKey(base string) string {
	base = strings.TrimSuffix(base, path.Ext(base))
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 2 {
		return strings.ReplaceAll(parts[1], "-", " ")
	}
	return base
}
