package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

const apiPrefix = "/movieBackup"

type Config struct {
	Port            string
	OSSEndpoint     string
	OSSRegion       string
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
	client *oss.Client
	bucket string
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	client, err := NewOSSClient(cfg)
	if err != nil {
		log.Fatalf("init oss client failed: %v", err)
	}

	s := &Server{cfg: cfg, client: client}
	mux := http.NewServeMux()
	mux.HandleFunc(apiPrefix+"/healthz", s.healthz)
	mux.HandleFunc(apiPrefix+"/api/backups", s.backupsEntry)
	mux.HandleFunc(apiPrefix+"/api/backups/", s.backupByIDEntry)

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
			log.Printf("config parse error: invalid MAX_UPLOAD_MB=%q", v)
			return Config{}, fmt.Errorf("invalid MAX_UPLOAD_MB")
		}
		maxMB = n
	}

	cfg := Config{
		Port:            envOrDefault("PORT", "8080"),
		OSSEndpoint:     strings.TrimSpace(os.Getenv("OSS_ENDPOINT")),
		OSSRegion:       strings.TrimSpace(os.Getenv("OSS_REGION")),
		OSSBucket:       strings.TrimSpace(os.Getenv("OSS_BUCKET")),
		OSSAccessKeyID:  strings.TrimSpace(os.Getenv("OSS_ACCESS_KEY_ID")),
		OSSAccessSecret: strings.TrimSpace(os.Getenv("OSS_ACCESS_KEY_SECRET")),
		OSSPrefix:       envOrDefault("OSS_PREFIX", "backups/"),
		MaxUploadMB:     maxMB,
	}

	if cfg.OSSEndpoint == "" || cfg.OSSBucket == "" || cfg.OSSAccessKeyID == "" || cfg.OSSAccessSecret == "" {
		log.Printf("config validation failed: required OSS env missing endpoint=%t bucket=%t accessKeyID=%t secret=%t",
			cfg.OSSEndpoint != "", cfg.OSSBucket != "", cfg.OSSAccessKeyID != "", cfg.OSSAccessSecret != "")
		return Config{}, errors.New("OSS_ENDPOINT, OSS_BUCKET, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET are required")
	}
	if !strings.HasSuffix(cfg.OSSPrefix, "/") {
		cfg.OSSPrefix += "/"
	}
	if cfg.OSSRegion == "" {
		if region, err := inferOSSRegion(cfg.OSSEndpoint); err == nil {
			cfg.OSSRegion = region
		} else {
			log.Printf("config parse error: infer OSS region failed endpoint=%q err=%v", cfg.OSSEndpoint, err)
			return Config{}, errors.New("invalid OSS endpoint: cannot infer region, set OSS_REGION explicitly")
		}
	}
	log.Printf("config loaded: port=%s oss_bucket=%s oss_endpoint=%s oss_region=%s prefix=%s max_upload_mb=%d",
		cfg.Port, cfg.OSSBucket, cfg.OSSEndpoint, cfg.OSSRegion, cfg.OSSPrefix, cfg.MaxUploadMB)

	return cfg, nil
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	if err := s.client.CheckConnection(s.cfg.OSSPrefix); err != nil {
		log.Printf("healthz degraded: oss unreachable prefix=%s err=%v", s.cfg.OSSPrefix, err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "degraded",
			"oss":    "unreachable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"oss":    "reachable",
	})
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
		log.Printf("upload request parse failed: err=%v max_upload_mb=%d", err, s.cfg.MaxUploadMB)
		writeError(w, http.StatusBadRequest, "invalid multipart form or payload too large")
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		log.Printf("upload validation failed: empty name")
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if !regexp.MustCompile(`^[\w\-\u4e00-\u9fa5 ]{1,64}$`).MatchString(name) {
		log.Printf("upload validation failed: invalid name=%q", name)
		writeError(w, http.StatusBadRequest, "name must be 1-64 chars and contains letters, numbers, spaces, -, _")
		return
	}
	log.Printf("upload started: name=%q", name)

	sqliteFile, sqliteHeader, err := r.FormFile("sqlite")
	if err != nil {
		log.Printf("upload validation failed: sqlite form file missing, err=%v", err)
		writeError(w, http.StatusBadRequest, "sqlite file is required (field: sqlite)")
		return
	}
	defer sqliteFile.Close()
	jsonFile, jsonHeader, err := r.FormFile("json")
	if err != nil {
		log.Printf("upload validation failed: json form file missing, err=%v", err)
		writeError(w, http.StatusBadRequest, "json file is required (field: json)")
		return
	}
	defer jsonFile.Close()

	archiveData, archiveName, err := buildArchive(name, sqliteHeader, sqliteFile, jsonHeader, jsonFile)
	if err != nil {
		log.Printf("build archive failed: name=%q sqlite=%q json=%q err=%v", name, sqliteHeader.Filename, jsonHeader.Filename, err)
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
		log.Printf("upload to OSS failed: key=%s err=%v", key, err)
		writeError(w, http.StatusInternalServerError, "upload to OSS failed")
		return
	}
	log.Printf("upload completed: id=%s key=%s archive_name=%s size=%d", id, key, archiveName, len(archiveData))

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         id,
		"name":       name,
		"created_at": now.Format(time.RFC3339),
		"object_key": key,
		"file_name":  archiveName,
	})
}

func buildArchive(name string, sqliteHeader *multipart.FileHeader, sqliteFile multipart.File, jsonHeader *multipart.FileHeader, jsonFile multipart.File) ([]byte, string, error) {
	log.Printf("build archive started: name=%q sqlite=%q json=%q", name, sqliteHeader.Filename, jsonHeader.Filename)
	sqliteData, err := io.ReadAll(sqliteFile)
	if err != nil {
		log.Printf("read sqlite failed: file=%q err=%v", sqliteHeader.Filename, err)
		return nil, "", errors.New("read sqlite failed")
	}
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Printf("read json failed: file=%q err=%v", jsonHeader.Filename, err)
		return nil, "", errors.New("read json failed")
	}
	if !json.Valid(jsonData) {
		log.Printf("json validation failed: file=%q", jsonHeader.Filename)
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
		log.Printf("zip close failed: name=%q err=%v", name, err)
		return nil, "", errors.New("build zip failed")
	}
	archiveName := fmt.Sprintf("%s-%s.zip", time.Now().UTC().Format("20060102T150405Z"), sanitizeName(name))
	log.Printf("build archive completed: name=%q archive_name=%s size=%d", name, archiveName, buf.Len())
	return buf.Bytes(), archiveName, nil
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
	log.Printf("list backups started: prefix=%s", s.cfg.OSSPrefix)
	for {
		res, err := s.client.ListObjects(s.cfg.OSSPrefix, marker, 1000)
		if err != nil {
			log.Printf("list backups failed: marker=%q err=%v", marker, err)
			writeError(w, http.StatusInternalServerError, "list objects failed")
			return
		}
		for _, obj := range res.Contents {
			key := oss.ToString(obj.Key)
			if strings.HasSuffix(key, "/") {
				continue
			}
			headers, err := s.client.HeadObject(key)
			if err != nil {
				log.Printf("head object skipped in list: key=%s err=%v", key, err)
				continue
			}
			name := headers.Get("X-Oss-Meta-Backup-Name")
			if name == "" {
				name = extractNameFromKey(path.Base(key))
			}
			id := headers.Get("X-Oss-Meta-Backup-Id")
			if id == "" {
				id = strings.TrimSuffix(path.Base(key), path.Ext(key))
			}
			createdAt := headers.Get("X-Oss-Meta-Backup-Created-At")
			if createdAt == "" {
				createdAt = oss.ToTime(obj.LastModified).UTC().Format(time.RFC3339)
			}
			items = append(items, BackupItem{ID: id, Name: name, CreatedAt: createdAt, Size: obj.Size, FileName: path.Base(key)})
		}
		if !res.IsTruncated {
			break
		}
		marker = oss.ToString(res.NextMarker)
	}

	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt > items[j].CreatedAt })
	log.Printf("list backups completed: count=%d", len(items))
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) backupByIDEntry(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.downloadBackup(w, r)
	case http.MethodDelete:
		s.deleteBackup(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) downloadBackup(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, apiPrefix+"/api/backups/"))
	if id == "" {
		log.Printf("download validation failed: empty backup id")
		writeError(w, http.StatusBadRequest, "backup id is required")
		return
	}
	log.Printf("download started: id=%s", id)
	key, fileName, err := s.findObjectByID(id)
	if err != nil {
		log.Printf("download failed: id=%s not found err=%v", id, err)
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	body, err := s.client.GetObject(key)
	if err != nil {
		log.Printf("download fetch failed: id=%s key=%s err=%v", id, key, err)
		writeError(w, http.StatusInternalServerError, "fetch backup failed")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, fileName))
	if _, err := io.Copy(w, body); err != nil {
		log.Printf("download stream failed: id=%s key=%s err=%v", id, key, err)
		return
	}
	log.Printf("download completed: id=%s key=%s file_name=%s", id, key, fileName)
}

func (s *Server) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, apiPrefix+"/api/backups/"))
	if id == "" {
		log.Printf("delete validation failed: empty backup id")
		writeError(w, http.StatusBadRequest, "backup id is required")
		return
	}
	log.Printf("delete started: id=%s", id)
	key, _, err := s.findObjectByID(id)
	if err != nil {
		log.Printf("delete failed: id=%s not found err=%v", id, err)
		writeError(w, http.StatusNotFound, "backup not found")
		return
	}
	if err := s.client.DeleteObject(key); err != nil {
		log.Printf("delete failed: id=%s key=%s err=%v", id, key, err)
		writeError(w, http.StatusInternalServerError, "delete backup failed")
		return
	}
	log.Printf("delete completed: id=%s key=%s", id, key)
	writeJSON(w, http.StatusOK, map[string]string{
		"id":      id,
		"message": "backup deleted",
	})
}

func (s *Server) findObjectByID(id string) (string, string, error) {
	marker := ""
	log.Printf("find object by id started: id=%s", id)
	for {
		res, err := s.client.ListObjects(s.cfg.OSSPrefix, marker, 1000)
		if err != nil {
			log.Printf("find object list failed: id=%s marker=%q err=%v", id, marker, err)
			return "", "", err
		}
		for _, obj := range res.Contents {
			key := oss.ToString(obj.Key)
			if strings.HasSuffix(key, "/") {
				continue
			}
			base := path.Base(key)
			if base == id || strings.TrimSuffix(base, path.Ext(base)) == id {
				log.Printf("find object matched by filename: id=%s key=%s", id, key)
				return key, base, nil
			}
			headers, err := s.client.HeadObject(key)
			if err == nil && headers.Get("X-Oss-Meta-Backup-Id") == id {
				log.Printf("find object matched by metadata: id=%s key=%s", id, key)
				return key, base, nil
			}
			if err != nil {
				log.Printf("find object head skipped: id=%s key=%s err=%v", id, key, err)
			}
		}
		if !res.IsTruncated {
			break
		}
		marker = oss.ToString(res.NextMarker)
	}
	log.Printf("find object not found: id=%s", id)
	return "", "", errors.New("not found")
}

func NewOSSClient(cfg Config) (*OSSClient, error) {
	endpoint := strings.TrimSpace(cfg.OSSEndpoint)
	if endpoint == "" {
		return nil, errors.New("oss endpoint is required")
	}

	os.Setenv("OSS_ACCESS_KEY_ID", cfg.OSSAccessKeyID)
	os.Setenv("OSS_ACCESS_KEY_SECRET", cfg.OSSAccessSecret)

	ossCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(cfg.OSSRegion).
		WithEndpoint(endpoint)
	client := oss.NewClient(ossCfg)

	return &OSSClient{client: client, bucket: cfg.OSSBucket}, nil
}

func (c *OSSClient) ListObjects(prefix, marker string, maxKeys int) (*oss.ListObjectsResult, error) {
	request := &oss.ListObjectsRequest{
		Bucket:  oss.Ptr(c.bucket),
		Prefix:  oss.Ptr(prefix),
		MaxKeys: int32(maxKeys),
	}
	if marker != "" {
		request.Marker = oss.Ptr(marker)
	}
	return c.client.ListObjects(context.TODO(), request)
}

func (c *OSSClient) PutObject(key string, body []byte, contentType string, headers map[string]string) error {
	request := &oss.PutObjectRequest{
		Bucket:   oss.Ptr(c.bucket),
		Key:      oss.Ptr(key),
		Body:     bytes.NewReader(body),
		Metadata: make(map[string]string),
	}
	if contentType != "" {
		request.ContentType = oss.Ptr(contentType)
	}
	for k, v := range headers {
		metaKey := strings.TrimPrefix(strings.ToLower(k), "x-oss-meta-")
		request.Metadata[metaKey] = v
	}
	_, err := c.client.PutObject(context.TODO(), request)
	return err
}

func (c *OSSClient) HeadObject(key string) (http.Header, error) {
	res, err := c.client.HeadObject(context.TODO(), &oss.HeadObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)})
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	for k, v := range res.Metadata {
		h.Set("X-Oss-Meta-"+k, v)
	}
	if v := oss.ToTime(res.LastModified); !v.IsZero() {
		h.Set("Last-Modified", v.UTC().Format(http.TimeFormat))
	}
	return h, nil
}

func (c *OSSClient) GetObject(key string) (io.ReadCloser, error) {
	res, err := c.client.GetObject(context.TODO(), &oss.GetObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)})
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func (c *OSSClient) DeleteObject(key string) error {
	_, err := c.client.DeleteObject(context.TODO(), &oss.DeleteObjectRequest{Bucket: oss.Ptr(c.bucket), Key: oss.Ptr(key)})
	return err
}

func (c *OSSClient) CheckConnection(prefix string) error {
	_, err := c.ListObjects(prefix, "", 1)
	return err
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

func inferOSSRegion(endpoint string) (string, error) {
	host := strings.TrimSpace(endpoint)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.Split(host, "/")[0]
	if host == "" {
		return "", errors.New("empty endpoint")
	}
	parts := strings.Split(host, ".")
	if len(parts) < 1 {
		return "", errors.New("invalid endpoint host")
	}
	segment := parts[0]
	if !strings.HasPrefix(segment, "oss-") {
		return "", fmt.Errorf("unsupported endpoint host %q", host)
	}
	region := strings.TrimPrefix(segment, "oss-")
	if region == "" {
		return "", fmt.Errorf("missing region in endpoint host %q", host)
	}
	return region, nil
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
